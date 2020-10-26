/*
Copyright 2020 CyVerse
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/iychoi/parcel/pkg/catalog"
	"github.com/iychoi/parcel/pkg/cli"
	"github.com/iychoi/parcel/pkg/kubernetes"
)

type CommandHandler func([]string)

type Command struct {
	Name        string
	Description string
	Handler     CommandHandler
}

var (
	commandList map[string]Command
	config      cli.Config
	trace       bool
	short       bool
)

func main() {
	var catalogServiceURL string
	var namespace string
	var kubernetesConfigPath string

	// read config
	if cli.CheckConfig() {
		config, err := cli.GetConfig()
		if err != nil {
			log.Fatal(err)
		}

		catalogServiceURL = config.CatalogServiceURL
		namespace = config.Namespace
		kubernetesConfigPath = config.KubernetesConfigPath
	}

	var version bool

	defaultKubeConfigPath, _ := kubernetes.GetHomeKubernetesConfigPath()

	// Parse parameters
	flag.BoolVar(&version, "version", false, "Print cli version information")
	flag.StringVar(&catalogServiceURL, "svcurl", catalog.CatalogServiceURL, "Set Catalog Service URL")
	flag.StringVar(&kubernetesConfigPath, "kubeconfig", defaultKubeConfigPath, "Set a kubernetes config path")
	flag.StringVar(&namespace, "namespace", kubernetes.VolumeNamespace, "Set a volume namespace")
	flag.BoolVar(&trace, "trace", false, "Trace communication with Catalog Service")
	flag.BoolVar(&short, "short", false, "Print short content")

	flag.Parse()

	// Handle Version
	if version {
		info, err := cli.GetVersionJSON()

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(info)
		os.Exit(0)
	}

	//log.Printf("Trace = %v\n", trace)
	initCommandHandlers()

	// set config
	config = cli.Config{
		CatalogServiceURL:    catalogServiceURL,
		Namespace:            namespace,
		KubernetesConfigPath: kubernetesConfigPath,
	}

	// save config file
	if !cli.CheckConfig() {
		err := cli.CreateConfig(&config)
		if err != nil {
			log.Fatal(err)
		}
	}

	args := flag.Args()

	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Give a command!\n")
		showCommands()
		os.Exit(1)
	}

	command := args[0]
	commandObject, containCommand := commandList[command]

	if !containCommand {
		fmt.Fprintf(os.Stderr, "Unknown command - %s\n", command)
		os.Exit(1)
	}

	commandObject.Handler(args[1:])

	os.Exit(0)
}

func initCommandHandlers() {
	commandList = map[string]Command{
		"help":    Command{"help", "show help message", helpHandler},
		"list":    Command{"list", "list available datasets", listHandler},
		"find":    Command{"find", "search datasets by keywords", searchHandler},
		"search":  Command{"search", "search datasets by keywords", searchHandler},
		"order":   Command{"order", "order a dataset", orderHandler},
		"mount":   Command{"mount", "order a dataset", orderHandler},
		"show":    Command{"show", "show orders", showHandler},
		"ps":      Command{"ps", "show orders", showHandler},
		"return":  Command{"return", "return a dataset", returnHandler},
		"unmount": Command{"unmount", "return a dataset", returnHandler},
	}
}

func showCommands() {
	for _, commandObj := range commandList {
		fmt.Printf("%s: %s\n", commandObj.Name, commandObj.Description)
	}
}

func listHandler(args []string) {
	client, err := catalog.NewCatalogServiceClient(config.CatalogServiceURL, trace)
	if err != nil {
		log.Fatal(err)
	}

	datasets, err := client.GetAllDatasets()
	if err != nil {
		log.Fatal(err)
	}

	for _, ds := range datasets {
		ds.PrintDataset(short, catalog.ShortDescriptionLen)
		fmt.Printf("\n")
	}
}

func searchHandler(args []string) {
	keywords := []string{}
	for _, arg := range args {
		if len(arg) < 4 {
			log.Printf("Keyword '%s' is ignored because it is too short", arg)
			continue
		}

		keywords = append(keywords, arg)
	}

	client, err := catalog.NewCatalogServiceClient(config.CatalogServiceURL, trace)
	if err != nil {
		log.Fatal(err)
	}

	datasets, err := client.SearchDatasets(keywords)
	if err != nil {
		log.Fatal(err)
	}

	for _, ds := range datasets {
		ds.PrintDataset(short, catalog.ShortDescriptionLen)
		fmt.Printf("\n")
	}
}

func orderHandler(args []string) {
	client, err := catalog.NewCatalogServiceClient(config.CatalogServiceURL, trace)
	if err != nil {
		log.Fatal(err)
	}

	datasets, err := client.SelectDatasets(args)
	if err != nil {
		log.Fatal(err)
	}

	volumeManager, err := kubernetes.NewVolumeManager(config.KubernetesConfigPath, config.Namespace)
	if err != nil {
		log.Fatal(err)
	}

	err = volumeManager.CreateStorageClass()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Ordering %d datasets...\n", len(datasets))
	for _, ds := range datasets {
		log.Printf("  Dataset: [%v] %s\n", ds.ID, ds.Name)

		mount, err := volumeManager.CreateVolume(ds)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("    VolumeName: %s\n", mount.PersistentVolume.GetName())
		log.Printf("    ClaimName: %s\n", mount.PersistentVolumeClaim.GetName())
	}
}

func showHandler(args []string) {
	volumeManager, err := kubernetes.NewVolumeManager(config.KubernetesConfigPath, config.Namespace)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Show orders...\n")
	mounts, err := volumeManager.ListVolumes()
	if err != nil {
		log.Fatal(err)
	}

	for _, mount := range mounts {
		log.Printf("  VolumeName: %s\n", mount.PersistentVolume.GetName())
		log.Printf("    Dataset: [%v] %s\n", mount.Dataset.ID, mount.Dataset.Name)
		log.Printf("    ClaimName: %s\n", mount.PersistentVolumeClaim.GetName())
	}
}

func returnHandler(args []string) {
	volumeManager, err := kubernetes.NewVolumeManager(config.KubernetesConfigPath, config.Namespace)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Returning datasets...\n")
	for _, volumeName := range args {
		mount, err := volumeManager.GetVolume(volumeName)
		if err != nil {
			log.Println(err)
		}

		log.Printf("  VolumeName: %s\n", mount.PersistentVolume.GetName())
		log.Printf("    Dataset: [%v] %s\n", mount.Dataset.ID, mount.Dataset.Name)
		log.Printf("    ClaimName: %s\n", mount.PersistentVolumeClaim.GetName())

		err = volumeManager.DeleteVolume(volumeName)
		if err != nil {
			log.Println(err)
		}
	}
}

func helpHandler(args []string) {
	showCommands()
}
