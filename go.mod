module github.com/iychoi/parcel

go 1.14

replace github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.3.1

require (
	github.com/go-resty/resty/v2 v2.3.0
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/iychoi/parcel-catalog-service v0.0.0-20201023193515-f2d77a6f91d4
	github.com/lithammer/shortuuid/v3 v3.0.4
	github.com/tkanos/gonfig v0.0.0-20181112185242-896f3d81fadf
	google.golang.org/appengine v1.6.1 // indirect
	k8s.io/api v0.17.0
	k8s.io/apimachinery v0.17.0
	k8s.io/client-go v0.17.0
	k8s.io/utils v0.0.0-20201015054608-420da100c033 // indirect
)
