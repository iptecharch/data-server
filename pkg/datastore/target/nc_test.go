// Copyright 2024 Nokia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package target

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/beevik/etree"
	"github.com/google/go-cmp/cmp"
	"github.com/sdcio/data-server/mocks/mocknetconf"
	"github.com/sdcio/data-server/mocks/mockschemaclientbound"
	"github.com/sdcio/data-server/pkg/config"
	SchemaClient "github.com/sdcio/data-server/pkg/datastore/clients/schema"
	"github.com/sdcio/data-server/pkg/datastore/target/netconf"
	"github.com/sdcio/data-server/pkg/datastore/target/netconf/types"
	"github.com/sdcio/data-server/pkg/utils/testhelper"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"go.uber.org/mock/gomock"
)

var (
	TestCtx = context.TODO()
)

func Test_ncTarget_Get(t *testing.T) {
	type fields struct {
		name            string
		getDriver       func(*gomock.Controller, *testing.T) netconf.Driver
		getSchemaClient func(*gomock.Controller, *testing.T) SchemaClient.SchemaClientBound
		sbiConfig       *config.SBI
	}
	type args struct {
		ctx context.Context
		req *sdcpb.GetDataRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *sdcpb.GetDataResponse
		wantErr bool
	}{
		{
			name: "One",
			fields: fields{
				getDriver: func(c *gomock.Controller, t *testing.T) netconf.Driver {
					d := mocknetconf.NewMockDriver(c)
					responseDoc := etree.NewDocument()
					err := responseDoc.ReadFromString("<data><interface><name>eth0</name><mtu>1500</mtu></interface></data>")
					if err != nil {
						t.Errorf("error creating response")
					}
					d.EXPECT().GetConfig("running", gomock.Any()).Return(&types.NetconfResponse{Doc: responseDoc}, nil)
					d.EXPECT().IsAlive().AnyTimes().Return(true)
					return d
				},
				name: "TestDev",
				sbiConfig: &config.SBI{
					NetconfOptions: &config.SBINetconfOptions{
						IncludeNS:              false,
						OperationWithNamespace: false,
						UseOperationRemove:     false,
					},
				},
				getSchemaClient: func(c *gomock.Controller, t *testing.T) SchemaClient.SchemaClientBound {
					s := mockschemaclientbound.NewMockSchemaClientBound(c)
					gomock.InOrder(
						s.EXPECT().GetSchema(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
								fmt.Println(path.String())
								return &sdcpb.GetSchemaResponse{
									Schema: &sdcpb.SchemaElem{
										Schema: &sdcpb.SchemaElem_Container{
											Container: &sdcpb.ContainerSchema{
												Name: "interface",
												Keys: []*sdcpb.LeafSchema{
													{
														Name: "name",
													},
												},
											},
										},
									},
								}, nil
							},
						),
						s.EXPECT().GetSchema(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
								fmt.Println(path.String())
								return &sdcpb.GetSchemaResponse{
									Schema: &sdcpb.SchemaElem{
										Schema: &sdcpb.SchemaElem_Container{
											Container: &sdcpb.ContainerSchema{
												Name: "interface",
												Keys: []*sdcpb.LeafSchema{
													{
														Name: "name",
													},
												},
											},
										},
									},
								}, nil
							},
						),
						s.EXPECT().GetSchema(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
								fmt.Println(path.String())
								return &sdcpb.GetSchemaResponse{
									Schema: &sdcpb.SchemaElem{
										Schema: &sdcpb.SchemaElem_Field{
											Field: &sdcpb.LeafSchema{
												Name: "name",
												Type: &sdcpb.SchemaLeafType{
													Type: "string",
												},
											},
										},
									},
								}, nil
							},
						),
						s.EXPECT().GetSchema(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
								fmt.Println(path.String())
								return &sdcpb.GetSchemaResponse{
									Schema: &sdcpb.SchemaElem{
										Schema: &sdcpb.SchemaElem_Field{
											Field: &sdcpb.LeafSchema{
												Name: "mtu",
												Type: &sdcpb.SchemaLeafType{
													Type: "string",
												},
											},
										},
									},
								}, nil
							},
						),
					)
					return s
				},
			},
			args: args{
				ctx: context.Background(),
				req: &sdcpb.GetDataRequest{
					Datastore: &sdcpb.DataStore{
						Type: sdcpb.Type_MAIN,
					},
					Path: []*sdcpb.Path{
						{
							Elem: []*sdcpb.PathElem{
								{
									Name: "interface",
									Key: map[string]string{
										"name": "eth0",
									},
								},
							},
						},
					},
				},
			},
			want: &sdcpb.GetDataResponse{
				Notification: []*sdcpb.Notification{
					{
						Update: []*sdcpb.Update{
							{
								Path: &sdcpb.Path{
									Elem: []*sdcpb.PathElem{
										{
											Name: "interface",
											Key: map[string]string{
												"name": "eth0",
											},
										},
										{
											Name: "name",
										},
									},
								},
								Value: &sdcpb.TypedValue{
									Value: &sdcpb.TypedValue_StringVal{
										StringVal: "eth0",
									},
								},
							},
							{
								Path: &sdcpb.Path{
									Elem: []*sdcpb.PathElem{
										{
											Name: "interface",
											Key: map[string]string{
												"name": "eth0",
											},
										},
										{
											Name: "mtu",
										},
									},
								},
								Value: &sdcpb.TypedValue{
									Value: &sdcpb.TypedValue_StringVal{
										StringVal: "1500",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create Mock controller
			mockCtrl := gomock.NewController(t)

			sc := tt.fields.getSchemaClient(mockCtrl, t)

			tr := &ncTarget{
				name:             tt.fields.name,
				driver:           tt.fields.getDriver(mockCtrl, t),
				schemaClient:     sc,
				sbiConfig:        tt.fields.sbiConfig,
				xml2sdcpbAdapter: netconf.NewXML2sdcpbConfigAdapter(sc),
			}
			got, err := tr.Get(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ncTarget.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ncTarget.Get() = %v, want %v", got, tt.want)
			}
			mockCtrl.Finish()
		})
	}
}

func TestLeafList(t *testing.T) {

	ctx := context.TODO()

	scb, err := getSchemaClientBound(t)
	if err != nil {
		t.Fatal(err)
	}

	leaflistValue := &sdcpb.TypedValue{
		Value: &sdcpb.TypedValue_LeaflistVal{
			LeaflistVal: &sdcpb.ScalarArray{
				Element: []*sdcpb.TypedValue{
					{
						Value: &sdcpb.TypedValue_StringVal{
							StringVal: "entry-one",
						},
					},
					{
						Value: &sdcpb.TypedValue_StringVal{
							StringVal: "entry-two",
						},
					},
					{
						Value: &sdcpb.TypedValue_StringVal{
							StringVal: "entry-three",
						},
					},
				},
			},
		},
	}

	xmlBuilder := netconf.NewXMLConfigBuilder(scb, &netconf.XMLConfigBuilderOpts{
		UseOperationRemove: true,
		HonorNamespace:     true,
	})

	xmlBuilder.AddValue(ctx, &sdcpb.Path{
		Elem: []*sdcpb.PathElem{
			{
				Name: "leaflist",
			},
			{
				Name: "entry",
			},
		},
	}, leaflistValue)

	expectedResult := `<leaflist xmlns="urn:sdcio/model" operation="replace">
  <entry>entry-one</entry>
  <entry>entry-two</entry>
  <entry>entry-three</entry>
</leaflist>
`
	result, err := xmlBuilder.GetDoc()
	if err != nil {
		t.Error(err)
	}
	if diff := cmp.Diff(result, expectedResult); diff != "" {
		t.Error(diff)
	}
}

// getSchemaClientBound creates a SchemaClientBound mock that responds to certain GetSchema requests
func getSchemaClientBound(t *testing.T) (SchemaClient.SchemaClientBound, error) {

	x, schema, err := testhelper.InitSDCIOSchema()
	if err != nil {
		return nil, err
	}

	sdcpbSchema := &sdcpb.Schema{
		Name:    schema.Name,
		Vendor:  schema.Vendor,
		Version: schema.Version,
	}

	mockCtrl := gomock.NewController(t)
	mockscb := mockschemaclientbound.NewMockSchemaClientBound(mockCtrl)

	// make the mock respond to GetSchema requests
	mockscb.EXPECT().GetSchema(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
			return x.GetSchema(ctx, &sdcpb.GetSchemaRequest{
				Path:   path,
				Schema: sdcpbSchema,
			})
		},
	)

	// setup the ToPath() responses
	mockscb.EXPECT().ToPath(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, path []string) (*sdcpb.Path, error) {
			pr, err := x.ToPath(ctx, &sdcpb.ToPathRequest{
				PathElement: path,
				Schema:      sdcpbSchema,
			})
			if err != nil {
				return nil, err
			}
			return pr.GetPath(), nil
		},
	)

	// return the mock
	return mockscb, nil
}

func Test_filterRPCErrors(t *testing.T) {

	xml := `
	<data>
	  <rpc-error>
		<error-severity>warning</error-severity>
		<error-message>warning1</error-message>
	  </rpc-error>
	  <rpc-error>
		<error-severity>warning</error-severity>
		<error-message>warning2</error-message>
	  </rpc-error>
	  <rpc-error>
		<error-severity>error</error-severity>
		<error-message>warning2</error-message>
	  </rpc-error>
	</data>
	`

	doc := etree.NewDocument()
	doc.ReadFromString(xml)

	type args struct {
		xml      *etree.Document
		severity string
	}
	tests := []struct {
		name    string
		args    args
		wantlen int
		wantErr bool
	}{
		{
			name: "one",
			args: args{
				xml:      doc,
				severity: "warning",
			},
			wantlen: 2,
			wantErr: false,
		},
		{
			name: "one",
			args: args{
				xml:      doc,
				severity: "error",
			},
			wantlen: 1,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := filterRPCErrors(tt.args.xml, tt.args.severity)
			if (err != nil) != tt.wantErr {
				t.Errorf("filterRPCErrors() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != tt.wantlen {
				t.Errorf("expected %d entries got %d", tt.wantlen, len(got))
			}
		})
	}
}
