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

package netconf

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/beevik/etree"
	"github.com/google/go-cmp/cmp"
	"github.com/sdcio/data-server/mocks/mockschemaclientbound"
	schemaClient "github.com/sdcio/data-server/pkg/datastore/clients/schema"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"go.uber.org/mock/gomock"
)

var (
	TestCtx = context.TODO()
)

func TestXMLConfigBuilder_GetDoc(t *testing.T) {
	tests := []struct {
		name          string
		getXmlBuilder func(ctrl *gomock.Controller) *XMLConfigBuilder
		want          string
		wantErr       bool
	}{
		{
			name: "Empty Document",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				return NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: false})
			},
			want:    ``,
			wantErr: false,
		},
		{
			name: "Non-Empty Document",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: false})

				// create doc elements
				people := cb.doc.CreateElement("get-config")
				interf := people.CreateElement("interfaces")
				interf.CreateAttr("name", "eth0")
				subinterf := interf.CreateElement("subinterface")
				subinterf.CreateAttr("name", "1")
				mtu := subinterf.CreateElement("vlan-id")
				mtu.SetText("5")
				return cb
			},
			want: `<get-config>
  <interfaces name="eth0">
    <subinterface name="1">
      <vlan-id>5</vlan-id>
    </subinterface>
  </interfaces>
</get-config>
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create Mock controller
			mockCtrl := gomock.NewController(t)

			got, err := tt.getXmlBuilder(mockCtrl).GetDoc()
			if (err != nil) != tt.wantErr {
				t.Errorf("XMLConfigBuilder.GetDoc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if d := cmp.Diff(got, tt.want); d != "" {
				t.Errorf("XMLConfigBuilder.GetDoc() = diff(%s)", d)
			}
			// signal mock done
			mockCtrl.Finish()
		})
	}
}

var (
	PathVlanId = &sdcpb.Path{
		Elem: []*sdcpb.PathElem{
			{Name: "get-config"},
			{Name: "interfaces"},
			{Name: "interface", Key: map[string]string{"name": "eth0"}},
			{Name: "subinterface", Key: map[string]string{"name": "1"}},
			{Name: "vlan-id"},
		},
	}
	PathVlanIdIF2 = &sdcpb.Path{
		Elem: []*sdcpb.PathElem{
			{Name: "get-config"},
			{Name: "interfaces"},
			{Name: "interface", Key: map[string]string{"name": "eth1"}},
			{Name: "subinterface", Key: map[string]string{"name": "5"}},
			{Name: "vlan-id"},
		},
	}
	PathInterfaces = &sdcpb.Path{
		Elem: []*sdcpb.PathElem{
			{Name: "get-config"},
			{Name: "interfaces"},
		},
	}
)

func TestXMLConfigBuilder_fastForward(t *testing.T) {
	Doc1 := etree.NewDocument()
	// populate Doc1
	getConfig := Doc1.CreateElement("get-config")
	interfs := getConfig.CreateElement("interfaces")
	interf1 := interfs.CreateElement("interface")
	interfname := interf1.CreateElement("name")
	interfname.SetText("eth0")
	mtuInterf1 := interf1.CreateElement("mtu")
	mtuInterf1.SetText("1500")
	subinterf11 := interf1.CreateElement("subinterface")
	subinterf11name := subinterf11.CreateElement("name")
	subinterf11name.SetText("1")
	vlanid := subinterf11.CreateElement("vlan-id")
	vlanid.SetText("5")
	interf2 := interfs.CreateElement("interface")
	interf2name := interf2.CreateElement("name")
	interf2name.SetText("eth1")
	mtuInterf2 := interf2.CreateElement("mtu")
	mtuInterf2.SetText("9100")
	// Doc1 done

	type args struct {
		ctx context.Context
		p   *sdcpb.Path
	}

	type testStruct struct {
		// The name of the test
		name string
		// Arguments to the fastforward function
		args args
		// rettrieve the initilaized xmlBuilder
		getXmlBuilder func(ctrl *gomock.Controller) *XMLConfigBuilder
		// check the results
		checkResult func(got *etree.Element, tt *testStruct, xmlBuilder *XMLConfigBuilder) error
		// indicate if an error is expeced or not
		wantErr bool
	}

	tests := []testStruct{
		{
			name: "Valid Document get vlan-id",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: false})
				cb.doc = Doc1
				return cb
			},
			checkResult: func(got *etree.Element, tt *testStruct, xmlbuilder *XMLConfigBuilder) error {
				want := vlanid
				if !reflect.DeepEqual(got, want) {
					return fmt.Errorf("XMLConfigBuilder.fastForward() = %v, want %v", got, want)
				}
				return nil
			},
			args: args{
				ctx: TestCtx,
				p:   PathVlanId,
			},
			wantErr: false,
		},
		{
			name: "Valid Document get interfaces",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: false})
				cb.doc = Doc1
				return cb
			},
			checkResult: func(got *etree.Element, tt *testStruct, xmlbuilder *XMLConfigBuilder) error {
				want := interfs
				if !reflect.DeepEqual(got, want) {
					return fmt.Errorf("XMLConfigBuilder.fastForward() = %v, want %v", got, want)
				}
				return nil
			},
			args: args{
				ctx: TestCtx,
				p:   PathInterfaces,
			},
			wantErr: false,
		},
		{
			name: "GetSchema Error",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				schemaClientMock.EXPECT().GetSchema(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
						return nil, fmt.Errorf("GetSchema Error")
					},
				)
				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: false})
				return cb
			},
			checkResult: func(got *etree.Element, tt *testStruct, xmlbuilder *XMLConfigBuilder) error {
				// no check required, we're expecting an error
				return nil
			},
			args: args{
				ctx: TestCtx,
				p:   PathInterfaces,
			},
			wantErr: true,
		},
		{
			name: "Non-Existing Path - Empty Namespace",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				schemaClientMock.EXPECT().GetSchema(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
						return &sdcpb.GetSchemaResponse{
							Schema: &sdcpb.SchemaElem{
								Schema: &sdcpb.SchemaElem_Container{
									Container: &sdcpb.ContainerSchema{
										Namespace: "",
									},
								},
							},
						}, nil
					},
				)

				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: false})
				return cb
			},
			checkResult: func(got *etree.Element, tt *testStruct, xmlbuilder *XMLConfigBuilder) error {
				expectedResult := `<get-config>
  <interfaces>
    <interface>
      <name>eth0</name>
      <subinterface>
        <name>1</name>
        <vlan-id/>
      </subinterface>
    </interface>
  </interfaces>
</get-config>
`
				xdoc, err := xmlbuilder.GetDoc()
				if err != nil {
					return err
				}
				if d := cmp.Diff(xdoc, expectedResult); d != "" {
					return fmt.Errorf("%s", d)
				}
				return nil
			},
			args: args{
				ctx: TestCtx,
				p:   PathVlanId,
			},
			wantErr: false,
		},
		{
			name: "Non-Existing Path - All different Namespaces",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				namespaceCounter := 0
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				schemaClientMock.EXPECT().GetSchema(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
						namespaceCounter++
						return &sdcpb.GetSchemaResponse{
							Schema: &sdcpb.SchemaElem{
								Schema: &sdcpb.SchemaElem_Container{
									Container: &sdcpb.ContainerSchema{
										Namespace: fmt.Sprintf("NS-%d", namespaceCounter),
									},
								},
							},
						}, nil
					},
				)

				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: true})
				return cb
			},
			checkResult: func(got *etree.Element, tt *testStruct, xmlbuilder *XMLConfigBuilder) error {
				expectedResult := `<get-config xmlns="NS-1">
  <interfaces xmlns="NS-2">
    <interface xmlns="NS-3">
      <name>eth0</name>
      <subinterface xmlns="NS-4">
        <name>1</name>
        <vlan-id xmlns="NS-5"/>
      </subinterface>
    </interface>
  </interfaces>
</get-config>
`
				xdoc, err := xmlbuilder.GetDoc()
				if err != nil {
					return err
				}
				if d := cmp.Diff(xdoc, expectedResult); d != "" {
					return fmt.Errorf("%s", d)
				}
				return nil
			},
			args: args{
				ctx: TestCtx,
				p:   PathVlanId,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create Mock controller
			mockCtrl := gomock.NewController(t)

			xmlBuilder := tt.getXmlBuilder(mockCtrl)
			got, err := xmlBuilder.fastForward(tt.args.ctx, tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("XMLConfigBuilder.fastForward() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err = tt.checkResult(got, &tt, xmlBuilder); err != nil {
				t.Error(err)
			}
			//fmt.Println(xmlBuilder.GetDoc())
			// signal mock done
			mockCtrl.Finish()
		})
	}
}

// TestXMLConfigBuilder_fastForward_multipleExecutions is to make sure that the fast-forwaring of paths with
// different Keys results in different branches in the xml tree
func TestXMLConfigBuilder_fastForward_multipleExecutions(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(mockCtrl)
	schemaClientMock.EXPECT().GetSchema(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
			return &sdcpb.GetSchemaResponse{
				Schema: &sdcpb.SchemaElem{
					Schema: &sdcpb.SchemaElem_Container{
						Container: &sdcpb.ContainerSchema{
							Namespace: "",
						},
					},
				},
			}, nil
		},
	)

	xmlbuilder := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: true})

	// fastForward the first path
	elem1, err := xmlbuilder.fastForward(TestCtx, PathVlanId)
	if err != nil {
		t.Error(err)
	}
	// fastForward the second path
	elem2, err := xmlbuilder.fastForward(TestCtx, PathVlanIdIF2)
	if err != nil {
		t.Error(err)
	}

	// make sure they did not resolve to the same element
	if elem1 == elem2 {
		t.Errorf("Should return two different elements")
	}

	// define the expected struct
	expectedResult := `<get-config>
  <interfaces>
    <interface>
      <name>eth0</name>
      <subinterface>
        <name>1</name>
        <vlan-id/>
      </subinterface>
    </interface>
    <interface>
      <name>eth1</name>
      <subinterface>
        <name>5</name>
        <vlan-id/>
      </subinterface>
    </interface>
  </interfaces>
</get-config>
`
	// retrieve the xml doc as string
	xdoc, err := xmlbuilder.GetDoc()
	if err != nil {
		t.Error(err)
	}
	// compare expected and retrieved
	if d := cmp.Diff(xdoc, expectedResult); d != "" {
		t.Errorf("%s", d)
	}
}

func TestXMLConfigBuilder_resolveNamespace(t *testing.T) {
	type args struct {
		ctx   context.Context
		p     *sdcpb.Path
		peIdx int
	}
	tests := []struct {
		name string
		// rettrieve the initilaized xmlBuilder
		getXmlBuilder func(ctrl *gomock.Controller) *XMLConfigBuilder
		args          args
		want          string
		wantErr       bool
	}{
		{
			name: "one",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				schemaClientMock.EXPECT().GetSchema(TestCtx, gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
						return &sdcpb.GetSchemaResponse{
							Schema: &sdcpb.SchemaElem{
								Schema: &sdcpb.SchemaElem_Container{
									Container: &sdcpb.ContainerSchema{
										Namespace: "SomeNamespace",
									},
								},
							},
						}, nil
					},
				)

				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: true})
				return cb
			},
			args: args{
				ctx:   TestCtx,
				p:     PathVlanId,
				peIdx: 4,
			},
			want:    "SomeNamespace",
			wantErr: false,
		},
		{
			name: "exceeding PE index - expect error",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				schemaClientMock.EXPECT().GetSchema(TestCtx, gomock.Any()).AnyTimes()

				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: true})
				return cb
			},
			args: args{
				ctx:   TestCtx,
				p:     PathVlanId,
				peIdx: 5,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "exceeding PE index - at max",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				schemaClientMock.EXPECT().GetSchema(TestCtx, gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
						return &sdcpb.GetSchemaResponse{
							Schema: &sdcpb.SchemaElem{
								Schema: &sdcpb.SchemaElem_Container{
									Container: &sdcpb.ContainerSchema{
										Namespace: "SomeNamespace",
									},
								},
							},
						}, nil
					},
				)

				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: true})
				return cb
			},
			args: args{
				ctx:   TestCtx,
				p:     PathVlanId,
				peIdx: 4,
			},
			want:    "SomeNamespace",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)

			x := tt.getXmlBuilder(mockCtrl)
			got, err := x.resolveNamespace(tt.args.ctx, tt.args.p, tt.args.peIdx)
			if (err != nil) != tt.wantErr {
				t.Errorf("XMLConfigBuilder.resolveNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("XMLConfigBuilder.resolveNamespace() = %v, want %v", got, tt.want)
			}
			mockCtrl.Finish()
		})
	}
}

func TestXMLConfigBuilder_AddValue(t *testing.T) {
	AddValueDoc1 := etree.NewDocument()
	// populate Doc1
	getConfig := AddValueDoc1.CreateElement("get-config")
	interfs := getConfig.CreateElement("interfaces")
	interf1 := interfs.CreateElement("interface")
	interfname := interf1.CreateElement("name")
	interfname.SetText("eth0")
	_ = interf1.CreateElement("mtu")
	subinterf11 := interf1.CreateElement("subinterface")
	subinterf11name := subinterf11.CreateElement("name")
	subinterf11name.SetText("1")
	_ = subinterf11.CreateElement("vlan-id")
	interf2 := interfs.CreateElement("interface")
	interf2name := interf2.CreateElement("name")
	interf2name.SetText("eth1")
	mtuInterf2 := interf2.CreateElement("mtu")
	mtuInterf2.SetText("9100")
	// Doc1 done

	type args struct {
		ctx context.Context
		p   *sdcpb.Path
		v   *sdcpb.TypedValue
	}

	type testStruct struct {
		// The name of the test
		name string
		// Arguments to the fastforward function
		args args
		// rettrieve the initilaized xmlBuilder
		getXmlBuilder func(ctrl *gomock.Controller) *XMLConfigBuilder
		// check the results
		checkResult func(tt *testStruct, xmlBuilder *XMLConfigBuilder) error
		// indicate if an error is expeced or not
		wantErr bool
	}

	tests := []testStruct{
		{
			name: "GetSchema Error",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				schemaClientMock.EXPECT().GetSchema(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
						return nil, fmt.Errorf("GetSchema Error")
					},
				)
				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: false})
				return cb
			},
			checkResult: func(tt *testStruct, xmlbuilder *XMLConfigBuilder) error {
				// no check required, we're expecting an error
				return nil
			},
			args: args{
				ctx: TestCtx,
				p:   PathInterfaces,
			},
			wantErr: true,
		},
		{
			name: "Non-Existing Path - Empty Namespace",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				schemaClientMock.EXPECT().GetSchema(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
						return &sdcpb.GetSchemaResponse{
							Schema: &sdcpb.SchemaElem{
								Schema: &sdcpb.SchemaElem_Container{
									Container: &sdcpb.ContainerSchema{
										Namespace: "",
									},
								},
							},
						}, nil
					},
				)

				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: false})
				return cb
			},
			checkResult: func(tt *testStruct, xmlbuilder *XMLConfigBuilder) error {
				expectedResult := `<get-config>
  <interfaces>
    <interface>
      <name>eth0</name>
      <subinterface>
        <name>1</name>
        <vlan-id>5</vlan-id>
      </subinterface>
    </interface>
  </interfaces>
</get-config>
`
				xdoc, err := xmlbuilder.GetDoc()
				if err != nil {
					return err
				}
				if d := cmp.Diff(xdoc, expectedResult); d != "" {
					return fmt.Errorf("%s", d)
				}
				return nil
			},
			args: args{
				ctx: TestCtx,
				p:   PathVlanId,
				v: &sdcpb.TypedValue{
					Value: &sdcpb.TypedValue_StringVal{
						StringVal: "5",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Non-Existing Path - All different Namespaces",
			getXmlBuilder: func(ctrl *gomock.Controller) *XMLConfigBuilder {
				namespaceCounter := 0
				schemaClientMock := mockschemaclientbound.NewMockSchemaClientBound(ctrl)
				schemaClientMock.EXPECT().GetSchema(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
					func(ctx context.Context, path *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
						namespaceCounter++
						return &sdcpb.GetSchemaResponse{
							Schema: &sdcpb.SchemaElem{
								Schema: &sdcpb.SchemaElem_Container{
									Container: &sdcpb.ContainerSchema{
										Namespace: fmt.Sprintf("NS-%d", namespaceCounter),
									},
								},
							},
						}, nil
					},
				)

				cb := NewXMLConfigBuilder(schemaClientMock, &XMLConfigBuilderOpts{HonorNamespace: true})
				return cb
			},
			checkResult: func(tt *testStruct, xmlbuilder *XMLConfigBuilder) error {
				expectedResult := `<get-config xmlns="NS-1">
  <interfaces xmlns="NS-2">
    <interface xmlns="NS-3">
      <name>eth0</name>
      <subinterface xmlns="NS-4">
        <name>1</name>
        <vlan-id xmlns="NS-6">5</vlan-id>
      </subinterface>
    </interface>
  </interfaces>
</get-config>
`
				xdoc, err := xmlbuilder.GetDoc()
				if err != nil {
					return err
				}
				if d := cmp.Diff(xdoc, expectedResult); d != "" {
					return fmt.Errorf("%s", d)
				}
				return nil
			},
			args: args{
				ctx: TestCtx,
				p:   PathVlanId,
				v: &sdcpb.TypedValue{
					Value: &sdcpb.TypedValue_StringVal{
						StringVal: "5",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create Mock controller
			mockCtrl := gomock.NewController(t)

			xmlBuilder := tt.getXmlBuilder(mockCtrl)
			err := xmlBuilder.AddValue(tt.args.ctx, tt.args.p, tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("XMLConfigBuilder.fastForward() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err = tt.checkResult(&tt, xmlBuilder); err != nil {
				t.Error(err)
			}
			//fmt.Println(xmlBuilder.GetDoc())
			// signal mock done
			mockCtrl.Finish()
		})
	}
}

func TestXMLConfigBuilder_Delete(t *testing.T) {

	GetNewDoc := func() *etree.Document {
		AddValueDoc1 := etree.NewDocument()
		// populate Doc1
		getConfig := AddValueDoc1.CreateElement("get-config")
		interfs := getConfig.CreateElement("interfaces")
		interf1 := interfs.CreateElement("interface")
		interfname := interf1.CreateElement("name")
		interfname.SetText("eth0")
		_ = interf1.CreateElement("mtu")
		subinterf11 := interf1.CreateElement("subinterface")
		subinterf11name := subinterf11.CreateElement("name")
		subinterf11name.SetText("1")
		_ = subinterf11.CreateElement("vlan-id")
		interf2 := interfs.CreateElement("interface")
		interf2name := interf2.CreateElement("name")
		interf2name.SetText("eth1")
		mtuInterf2 := interf2.CreateElement("mtu")
		mtuInterf2.SetText("9100")
		return AddValueDoc1
		// Doc1 done
	}

	type fields struct {
		cfg          *XMLConfigBuilderOpts
		doc          *etree.Document
		schemaClient schemaClient.SchemaClientBound
	}
	type args struct {
		ctx context.Context
		p   *sdcpb.Path
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    string
	}{
		{
			name: "Delete with namespace",
			fields: fields{
				doc: GetNewDoc(),
				cfg: &XMLConfigBuilderOpts{
					HonorNamespace:         true,
					OperationWithNamespace: true,
					UseOperationRemove:     false,
				},
			},
			args: args{
				ctx: TestCtx,
				p:   PathInterfaces,
			},
			wantErr: false,
			want: `<get-config>
  <interfaces xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0" nc:operation="delete">
    <interface>
      <name>eth0</name>
      <mtu/>
      <subinterface>
        <name>1</name>
        <vlan-id/>
      </subinterface>
    </interface>
    <interface>
      <name>eth1</name>
      <mtu>9100</mtu>
    </interface>
  </interfaces>
</get-config>
`,
		},
		{
			name: "Delete with namespace",
			fields: fields{
				doc: GetNewDoc(),
				cfg: &XMLConfigBuilderOpts{
					HonorNamespace:         false,
					OperationWithNamespace: false,
					UseOperationRemove:     false,
				},
			},
			args: args{
				ctx: TestCtx,
				p:   PathInterfaces,
			},
			wantErr: false,
			want: `<get-config>
  <interfaces operation="delete">
    <interface>
      <name>eth0</name>
      <mtu/>
      <subinterface>
        <name>1</name>
        <vlan-id/>
      </subinterface>
    </interface>
    <interface>
      <name>eth1</name>
      <mtu>9100</mtu>
    </interface>
  </interfaces>
</get-config>
`,
		},
		{
			name: "Remove with namespace",
			fields: fields{
				doc: GetNewDoc(),
				cfg: &XMLConfigBuilderOpts{
					HonorNamespace:         true,
					OperationWithNamespace: true,
					UseOperationRemove:     true,
				},
			},
			args: args{
				ctx: TestCtx,
				p:   PathInterfaces,
			},
			wantErr: false,
			want: `<get-config>
  <interfaces xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0" nc:operation="remove">
    <interface>
      <name>eth0</name>
      <mtu/>
      <subinterface>
        <name>1</name>
        <vlan-id/>
      </subinterface>
    </interface>
    <interface>
      <name>eth1</name>
      <mtu>9100</mtu>
    </interface>
  </interfaces>
</get-config>
`,
		},
		{
			name: "Remove with namespace",
			fields: fields{
				doc: GetNewDoc(),
				cfg: &XMLConfigBuilderOpts{
					HonorNamespace:         false,
					OperationWithNamespace: false,
					UseOperationRemove:     true,
				},
			},
			args: args{
				ctx: TestCtx,
				p:   PathInterfaces,
			},
			wantErr: false,
			want: `<get-config>
  <interfaces operation="remove">
    <interface>
      <name>eth0</name>
      <mtu/>
      <subinterface>
        <name>1</name>
        <vlan-id/>
      </subinterface>
    </interface>
    <interface>
      <name>eth1</name>
      <mtu>9100</mtu>
    </interface>
  </interfaces>
</get-config>
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := &XMLConfigBuilder{
				cfg:          tt.fields.cfg,
				doc:          tt.fields.doc,
				schemaClient: tt.fields.schemaClient,
			}
			if err := x.Delete(tt.args.ctx, tt.args.p); (err != nil) != tt.wantErr {
				t.Errorf("XMLConfigBuilder.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
			doc, err := x.GetDoc()
			if err != nil {
				t.Error(err)
			}
			fmt.Println(doc)
			if diff := cmp.Diff(doc, tt.want); diff != "" {
				t.Errorf("%s", diff)
			}
		})
	}
}
