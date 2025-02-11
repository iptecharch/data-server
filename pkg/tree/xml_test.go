package tree

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/openconfig/ygot/ygot"
	"github.com/sdcio/data-server/mocks/mockcacheclient"
	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/utils"
	"github.com/sdcio/data-server/pkg/utils/testhelper"
	sdcio_schema "github.com/sdcio/data-server/tests/sdcioygot"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"go.uber.org/mock/gomock"
)

func TestToXMLTable(t *testing.T) {
	var tests = []struct {
		name                   string
		onlyNewOrUpdated       bool
		honorNamespace         bool
		operationWithNamespace bool
		useOperationRemove     bool
		existingConfig         func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error)
		runningConfig          func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error)
		newConfig              func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error)
		expected               string
	}{
		{
			name:             "XML All",
			onlyNewOrUpdated: false,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `<choices>
  <case1>
    <case-elem>
      <elem>foocaseval</elem>
    </case-elem>
  </case1>
</choices>
<interface>
  <admin-state>enable</admin-state>
  <description>Foo</description>
  <name>ethernet-1/1</name>
  <subinterface>
    <description>Subinterface 0</description>
    <index>0</index>
    <type>sdcio_model_common:routed</type>
  </subinterface>
</interface>
<leaflist>
  <entry>foo</entry>
  <entry>bar</entry>
</leaflist>
<network-instance>
  <admin-state>disable</admin-state>
  <description>Default NI</description>
  <name>default</name>
  <type>sdcio_model_ni:default</type>
</network-instance>
<patterntest>foo</patterntest>
`,
		},
		{
			name:             "XML - no new",
			onlyNewOrUpdated: true,
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: ``,
		},
		{
			name:             "XML NewOrUpdated - some elements deleted, some updated",
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			newConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config2()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `<choices operation="delete"/>
<interface operation="delete">
  <name>ethernet-1/1</name>
</interface>
<interface>
  <admin-state>enable</admin-state>
  <description>Foo</description>
  <name>ethernet-1/2</name>
  <subinterface>
    <description>Subinterface 5</description>
    <index>5</index>
    <type>sdcio_model_common:routed</type>
  </subinterface>
</interface>
<leaflist operation="delete"/>
<network-instance operation="delete">
  <name>default</name>
</network-instance>
<network-instance>
  <admin-state>enable</admin-state>
  <description>Other NI</description>
  <name>other</name>
  <type>sdcio_model_ni:ip-vrf</type>
</network-instance>
<patterntest>bar</patterntest>
`,
		},
		{
			name:             "XML - delete ethernet-1/1, honor namespace, operatin With namespace, remove",
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `<interface xmlns="urn:sdcio/model" xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0" nc:operation="remove">
  <name>ethernet-1/1</name>
</interface>
`,
			honorNamespace:         true,
			operationWithNamespace: true,
			useOperationRemove:     true,
			newConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				delete(c.Interface, "ethernet-1/1")
				return expandUpdateFromConfig(ctx, c, converter)
			},
		},
		{
			name:             "XML - honor namespace, operatin With namespace",
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `<interface xmlns="urn:sdcio/model" xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0" nc:operation="delete">
  <name>ethernet-1/1</name>
</interface>
`,
			honorNamespace:         true,
			operationWithNamespace: true,
			useOperationRemove:     false,
			newConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				delete(c.Interface, "ethernet-1/1")
				return expandUpdateFromConfig(ctx, c, converter)
			},
		},
		{
			name:             "XML - delete certain ethernet-1/1 attributes update another",
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `<interface xmlns="urn:sdcio/model">
  <description xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0" nc:operation="remove"/>
  <name>ethernet-1/1</name>
  <subinterface>
    <description xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0" nc:operation="remove"/>
    <index>0</index>
    <type>sdcio_model_common:bridged</type>
  </subinterface>
</interface>
`,
			honorNamespace:         true,
			operationWithNamespace: true,
			useOperationRemove:     true,
			newConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				c.Interface["ethernet-1/1"].Description = nil
				c.Interface["ethernet-1/1"].Subinterface[0].Description = nil
				c.Interface["ethernet-1/1"].Subinterface[0].Type = sdcio_schema.SdcioModelCommon_SiType_bridged
				return expandUpdateFromConfig(ctx, c, converter)
			},
		},
		{
			name:             "XML - delete ethernet-1/1 add ethernet-1/2",
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `<choices>
  <case1 operation="delete"/>
</choices>
<interface operation="delete">
  <name>ethernet-1/1</name>
</interface>
<interface>
  <admin-state>enable</admin-state>
  <description>Test</description>
  <name>ethernet-1/2</name>
</interface>
<patterntest operation="delete"/>
`,
			honorNamespace:         false,
			operationWithNamespace: false,
			useOperationRemove:     false,
			newConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				delete(c.Interface, "ethernet-1/1")
				c.Interface["ethernet-1/2"] = &sdcio_schema.SdcioModel_Interface{
					AdminState:  sdcio_schema.SdcioModelIf_AdminState_enable,
					Name:        ygot.String("ethernet-1/2"),
					Description: ygot.String("Test"),
				}
				c.Patterntest = nil
				c.Choices.Case1.CaseElem.Elem = nil
				return expandUpdateFromConfig(ctx, c, converter)
			},
		},
		{
			name:             "XML - replace direct leaf and choice",
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `<choices xmlns="urn:sdcio/model">
  <case1 xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0" nc:operation="remove"/>
  <case2>
    <log>true</log>
  </case2>
</choices>
<patterntest xmlns:nc="urn:ietf:params:xml:ns:netconf:base:1.0" nc:operation="remove"/>
`,
			honorNamespace:         true,
			operationWithNamespace: true,
			useOperationRemove:     true,
			newConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				c.Patterntest = nil
				c.Choices.Case1.CaseElem.Elem = nil
				c.Choices.Case2 = &sdcio_schema.SdcioModel_Choices_Case2{
					Log: ygot.Bool(true),
				}
				return expandUpdateFromConfig(ctx, c, converter)
			},
		},
		{
			name:             "XML - empty",
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			newConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()

				upds, err := expandUpdateFromConfig(ctx, c, converter)
				if err != nil {
					return nil, err
				}
				upds = append(upds, &sdcpb.Update{Path: &sdcpb.Path{Elem: []*sdcpb.PathElem{{Name: "emptyconf"}}}, Value: &sdcpb.TypedValue{Value: &sdcpb.TypedValue_EmptyVal{}}})
				return upds, nil
			},
			expected: `<emptyconf/>
`,
			honorNamespace:         true,
			operationWithNamespace: true,
			useOperationRemove:     true,
		},
		{
			name:             "XML - presence",
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `<network-instance xmlns="urn:sdcio/model">
  <name>default</name>
  <protocol>
    <bgp/>
  </protocol>
</network-instance>
`,
			honorNamespace:         true,
			operationWithNamespace: true,
			useOperationRemove:     true,
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			newConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				upds, err := expandUpdateFromConfig(ctx, c, converter)
				if err != nil {
					return nil, err
				}
				upds = append(upds, &sdcpb.Update{
					Path: &sdcpb.Path{
						Elem: []*sdcpb.PathElem{
							{Name: "network-instance", Key: map[string]string{"name": "default"}},
							{Name: "protocol"},
							{Name: "bgp"},
						},
					},
					Value: &sdcpb.TypedValue{Value: &sdcpb.TypedValue_EmptyVal{}},
				})
				return upds, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			// mock schema client
			scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
			if err != nil {
				t.Fatal(err)
			}
			owner := "owner1"

			// mock cache client
			ccMock := mockcacheclient.NewMockClient(mockCtrl)
			testhelper.ConfigureCacheClientMock(t, ccMock, []*cache.Update{}, []*cache.Update{}, []*cache.Update{}, [][]string{})

			ctx := context.Background()

			converter := utils.NewConverter(scb)

			tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", ccMock, scb), owner)
			root, err := NewTreeRoot(ctx, tc)
			if err != nil {
				t.Fatal(err)
			}
			if tt.existingConfig != nil {
				existingUpds, err := tt.existingConfig(ctx, converter)
				if err != nil {
					t.Error(err)
				}
				err = addToRoot(ctx, root, existingUpds, flagsExisting, owner, 5)
				if err != nil {
					t.Fatal(err)
				}
			}

			if tt.newConfig != nil {
				root.markOwnerDelete(owner, false)

				newUpds, err := tt.newConfig(ctx, converter)
				if err != nil {
					t.Error(err)
				}
				err = addToRoot(ctx, root, newUpds, flagsNew, owner, 5)
				if err != nil {
					t.Fatal(err)
				}
			}
			if tt.runningConfig != nil {
				runningUpds, err := tt.runningConfig(ctx, converter)
				if err != nil {
					t.Error(err)
				}
				err = addToRoot(ctx, root, runningUpds, flagsExisting, RunningIntentName, RunningValuesPrio)
				if err != nil {
					t.Fatal(err)
				}
			}
			root.FinishInsertionPhase(ctx)
			// fmt.Println(root.String())

			xmlDoc, err := root.ToXML(tt.onlyNewOrUpdated, tt.honorNamespace, tt.operationWithNamespace, tt.useOperationRemove)
			if err != nil {
				t.Fatal(err)
			}

			// Make sure the attributes are sorted, otherwise the comparison is an issue
			utils.XmlRecursiveSortElementsByTagName(&xmlDoc.Element)

			xmlDoc.Indent(2)
			xmlDocStr, err := xmlDoc.WriteToString()
			if err != nil {
				t.Fatal(err)
			}

			fmt.Println(string(xmlDocStr))

			if diff := cmp.Diff(tt.expected, string(xmlDocStr)); diff != "" {
				t.Fatalf("ToXML() failed.\nDiff:\n%s", diff)
			}
		})
	}
}
