package route53

import (
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	"github.com/gliderlabs/registrator/bridge"
)

func init() {
	bridge.Register(new(Factory), "route53")
}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	svc := route53.New(session.New(), &aws.Config{})

	if len(uri.Path) < 2 {
		log.Fatal("route53: dns domain required e.g.: route53://<host>/<domain>")
	}

	return &Route53Adapter{
		hostedZoneId: uri.Host,
		route53:      svc,
		dnsName:      strings.Split(uri.Path, "/")[1],
	}
}

type Route53Adapter struct {
	hostedZoneId string
	dnsName      string
	route53      *route53.Route53
}

func (r *Route53Adapter) Ping() error {
	return nil
}

func (r *Route53Adapter) Register(service *bridge.Service) error {
	currentName := fmt.Sprintf("%s.%s.", service.Name, r.dnsName)
	resp, err := r.route53.ListResourceRecordSets(&route53.ListResourceRecordSetsInput{
		HostedZoneId:    aws.String(r.hostedZoneId),
		StartRecordName: aws.String(currentName),
		StartRecordType: aws.String("SRV"),
		MaxItems:        aws.String("1000"),
	})

	if err != nil {
		return nil
	}

	recordSet := fmt.Sprintf("1 1 %d %s.", service.Port, service.IP)

	changeSet := []*route53.ResourceRecord{}
	for _, record := range resp.ResourceRecordSets {
		if strings.Compare(*record.Name, currentName) == 0 {
			// the current recordName is what we are looking for!
			// copy the remote configuration
			for _, r := range record.ResourceRecords {
				if strings.Compare(*(r.Value), recordSet) == 0 {
					log.Printf("Record '%s' already present\n", recordSet)
					return nil
				}

				changeSet = append(changeSet, r)
			}
			break
		}
	}

	// Added a new record
	changeSet = append(changeSet, &route53.ResourceRecord{
		Value: aws.String(recordSet),
	})
	_, err = r.route53.ChangeResourceRecordSets(&route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(r.hostedZoneId),
		ChangeBatch: &route53.ChangeBatch{
			Changes: []*route53.Change{
				{
					Action: aws.String("UPSERT"),
					ResourceRecordSet: &route53.ResourceRecordSet{
						Name:            aws.String(fmt.Sprintf("%s.%s", service.Name, r.dnsName)),
						Type:            aws.String("SRV"),
						ResourceRecords: changeSet,
						TTL:             aws.Int64(int64(service.TTL)),
					},
				},
			},
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func (r *Route53Adapter) Deregister(service *bridge.Service) error {
	currentName := fmt.Sprintf("%s.%s.", service.Name, r.dnsName)
	resp, err := r.route53.ListResourceRecordSets(&route53.ListResourceRecordSetsInput{
		HostedZoneId:    aws.String(r.hostedZoneId),
		StartRecordName: aws.String(currentName),
		StartRecordType: aws.String("SRV"),
		MaxItems:        aws.String("1000"),
	})

	if err != nil {
		return nil
	}

	recordSet := fmt.Sprintf("1 1 %d %s.", service.Port, service.IP)

	changeSet := []*route53.ResourceRecord{}
	for _, record := range resp.ResourceRecordSets {
		if strings.Compare(*record.Name, currentName) == 0 {
			// the current recordName is what we are looking for!
			// copy the remote configuration
			for i, res := range record.ResourceRecords {
				if strings.Compare(*(res.Value), recordSet) == 0 {
					if len(record.ResourceRecords) == 1 {
						_, err := r.route53.ChangeResourceRecordSets(&route53.ChangeResourceRecordSetsInput{
							HostedZoneId: aws.String(r.hostedZoneId),
							ChangeBatch: &route53.ChangeBatch{
								Changes: []*route53.Change{
									{
										Action: aws.String("DELETE"),
										ResourceRecordSet: &route53.ResourceRecordSet{
											Name: aws.String(fmt.Sprintf("%s.%s", service.Name, r.dnsName)),
											Type: aws.String("SRV"),
											ResourceRecords: []*route53.ResourceRecord{
												{
													Value: aws.String(fmt.Sprintf("1 1 %d %s.", service.Port, service.IP)),
												},
											},
											TTL: aws.Int64(int64(service.TTL)),
										},
									},
								},
							},
						})

						if err != nil {
							return err
						}

						return nil
					}

					changeSet = append(record.ResourceRecords[:i], record.ResourceRecords[i+1:]...)
					_, err = r.route53.ChangeResourceRecordSets(&route53.ChangeResourceRecordSetsInput{
						HostedZoneId: aws.String(r.hostedZoneId),
						ChangeBatch: &route53.ChangeBatch{
							Changes: []*route53.Change{
								{
									Action: aws.String("UPSERT"),
									ResourceRecordSet: &route53.ResourceRecordSet{
										Name:            aws.String(fmt.Sprintf("%s.%s", service.Name, r.dnsName)),
										Type:            aws.String("SRV"),
										ResourceRecords: changeSet,
										TTL:             aws.Int64(int64(service.TTL)),
									},
								},
							},
						},
					})

					if err != nil {
						return err
					}

					return nil
				}
			}
			break
		}
	}

	return nil
}

func (r *Route53Adapter) Refresh(service *bridge.Service) error {
	return nil
}

func (r *Route53Adapter) Services() ([]*bridge.Service, error) {
	return []*bridge.Service{}, nil
}
