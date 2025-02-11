package tree

import (
	"context"
	"encoding/json"
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
	"google.golang.org/protobuf/proto"
)

func TestToJsonTable(t *testing.T) {

	var tests = []struct {
		name             string
		ietf             bool
		onlyNewOrUpdated bool
		existingConfig   func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error)
		runningConfig    func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error)
		newConfig        func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error)
		expected         string
	}{
		{
			name:             "JSON All",
			ietf:             false,
			onlyNewOrUpdated: false,
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `{
  "choices": {
    "case1": {
      "case-elem": {
        "elem": "foocaseval"
      }
    }
  },
  "interface": [
    {
      "admin-state": "enable",
      "description": "Foo",
      "name": "ethernet-1/1",
      "subinterface": [
        {
          "description": "Subinterface 0",
          "index": 0,
          "type": "routed"
        }
      ]
    }
  ],
  "leaflist": {
    "entry": [
      "foo",
      "bar"
    ]
  },
  "network-instance": [
    {
      "admin-state": "disable",
      "description": "Default NI",
      "name": "default",
      "type": "default"
    }
  ],
  "patterntest": "foo"
}`,
		},
		{
			name:             "JsonIETF All",
			ietf:             true,
			onlyNewOrUpdated: false,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `{
  "sdcio_model:patterntest": "foo",
  "sdcio_model_choice:choices": {
    "case1": {
      "case-elem": {
        "elem": "foocaseval"
      }
    }
  },
  "sdcio_model_if:interface": [
    {
      "admin-state": "enable",
      "description": "Foo",
      "name": "ethernet-1/1",
      "subinterface": [
        {
          "description": "Subinterface 0",
          "index": 0,
          "type": "sdcio_model_common:routed"
        }
      ]
    }
  ],
  "sdcio_model_leaflist:leaflist": {
    "entry": [
      "foo",
      "bar"
    ]
  },
  "sdcio_model_ni:network-instance": [
    {
      "admin-state": "disable",
      "description": "Default NI",
      "name": "default",
      "type": "sdcio_model_ni:default"
    }
  ]
}`,
		},
		{
			name:             "JSON NewOrUpdated - no new",
			ietf:             false,
			onlyNewOrUpdated: true,
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},

			expected: `{}`,
		},
		{
			name:             "JSON_IETF NewOrUpdated - no new",
			ietf:             true,
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},

			expected: `{}`,
		},
		{
			name:             "JSON NewOrUpdated - with new",
			ietf:             false,
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
				c := config2()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `{
  "interface": [
    {
      "admin-state": "enable",
      "description": "Foo",
      "name": "ethernet-1/2",
      "subinterface": [
        {
          "description": "Subinterface 5",
          "index": 5,
          "type": "routed"
        }
      ]
    }
  ],
  "network-instance": [
    {
      "admin-state": "enable",
      "description": "Other NI",
      "name": "other",
      "type": "ip-vrf"
    }
  ],
  "patterntest": "bar"
}`,
		},
		{
			name:             "JSON_IETF NewOrUpdated - with new",
			ietf:             true,
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
				c := config2()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `{
  "sdcio_model:patterntest": "bar",
  "sdcio_model_if:interface": [
    {
      "admin-state": "enable",
      "description": "Foo",
      "name": "ethernet-1/2",
      "subinterface": [
        {
          "description": "Subinterface 5",
          "index": 5,
          "type": "sdcio_model_common:routed"
        }
      ]
    }
  ],
  "sdcio_model_ni:network-instance": [
    {
      "admin-state": "enable",
      "description": "Other NI",
      "name": "other",
      "type": "sdcio_model_ni:ip-vrf"
    }
  ]
}`,
		},
		{
			name:             "JSON_IETF - int16",
			ietf:             true,
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
				c.Interface["ethernet-1/1"].Mtu = ygot.Uint16(1500)
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `{
  "sdcio_model_if:interface": [
    {
      "mtu": 1500,
      "name": "ethernet-1/1"
    }
  ]
}`,
		},
		{
			name:             "JSON - presence",
			onlyNewOrUpdated: true,
			existingConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			runningConfig: func(ctx context.Context, converter *utils.Converter) ([]*sdcpb.Update, error) {
				c := config1()
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `{
  "network-instance": [
    {
      "name": "default",
      "protocol": {
        "bgp": {}
      }
    }
  ]
}`,
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
		{
			name:             "JSON - empty",
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
			expected: `{
  "emptyconf": {}
}`,
		},
		{
			name:             "JSON_IETF - identityref",
			ietf:             true,
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
				c.Identityref = &sdcio_schema.SdcioModel_Identityref{
					CryptoA: sdcio_schema.SdcioModelIdentityBase_CryptoAlg_des3,
					CryptoB: sdcio_schema.SdcioModelIdentityBase_CryptoAlg_otherAlgo,
				}
				return expandUpdateFromConfig(ctx, c, converter)
			},
			expected: `{
  "sdcio_model_identity:identityref": {
    "cryptoA": "sdcio_model_identity_types:des3",
    "cryptoB": "sdcio_model_identity:otherAlgo"
  }
}`,
		},
	}

	flagsNew := NewUpdateInsertFlags()
	flagsNew.SetNewFlag()

	flagsOld := NewUpdateInsertFlags()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			mockCtrl := gomock.NewController(t)

			scb, err := testhelper.GetSchemaClientBound(t, mockCtrl)
			if err != nil {
				t.Fatal(err)
			}

			owner := "owner1"

			ctx := context.Background()

			// create a cache client mock
			cacheClient := mockcacheclient.NewMockClient(mockCtrl)
			testhelper.ConfigureCacheClientMock(t, cacheClient, []*cache.Update{}, []*cache.Update{}, []*cache.Update{}, [][]string{})

			tc := NewTreeContext(NewTreeSchemaCacheClient("dev1", cacheClient, scb), owner)
			root, err := NewTreeRoot(ctx, tc)
			if err != nil {
				t.Fatal(err)
			}
			converter := utils.NewConverter(scb)

			if tt.runningConfig != nil {
				updsRunning, err := tt.runningConfig(ctx, converter)
				if err != nil {
					t.Error(err)
				}

				err = addToRoot(ctx, root, updsRunning, flagsOld, RunningIntentName, RunningValuesPrio)
				if err != nil {
					t.Fatal(err)
				}
			}

			updsExisting, err := tt.existingConfig(ctx, converter)
			if err != nil {
				t.Error(err)
			}

			err = addToRoot(ctx, root, updsExisting, flagsOld, owner, 5)
			if err != nil {
				t.Fatal(err)
			}

			if tt.newConfig != nil {
				updsNew, err := tt.newConfig(ctx, converter)
				if err != nil {
					t.Error(err)
				}
				err = addToRoot(ctx, root, updsNew, flagsNew, owner, 5)
				if err != nil {
					t.Fatal(err)
				}
			}
			root.FinishInsertionPhase(ctx)

			fmt.Println(root.String())

			var jsonStruct any

			if tt.ietf {
				jsonStruct, err = root.ToJsonIETF(tt.onlyNewOrUpdated)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				jsonStruct, err = root.ToJson(tt.onlyNewOrUpdated)
				if err != nil {
					t.Fatal(err)
				}
			}

			jsonStr, err := json.MarshalIndent(jsonStruct, "", "  ")
			if err != nil {
				t.Fatal(err)
			}

			fmt.Println(string(jsonStr))

			if diff := cmp.Diff(tt.expected, string(jsonStr)); diff != "" {
				var ietfStr = ""
				if tt.ietf {
					ietfStr = "IETF"
				}
				t.Fatalf("ToJson%s() failed.\nDiff:\n%s", ietfStr, diff)
			}

			mockCtrl.Finish()
		})
	}
}

func config1() *sdcio_schema.Device {
	return &sdcio_schema.Device{
		Interface: map[string]*sdcio_schema.SdcioModel_Interface{
			"ethernet-1/1": {
				AdminState:  sdcio_schema.SdcioModelIf_AdminState_enable,
				Description: ygot.String("Foo"),
				Name:        ygot.String("ethernet-1/1"),
				Subinterface: map[uint32]*sdcio_schema.SdcioModel_Interface_Subinterface{
					0: {
						Description: ygot.String("Subinterface 0"),
						Type:        sdcio_schema.SdcioModelCommon_SiType_routed,
						Index:       ygot.Uint32(0),
					},
				},
			},
		},
		Choices: &sdcio_schema.SdcioModel_Choices{
			Case1: &sdcio_schema.SdcioModel_Choices_Case1{
				CaseElem: &sdcio_schema.SdcioModel_Choices_Case1_CaseElem{
					Elem: ygot.String("foocaseval"),
				},
			},
		},
		Leaflist: &sdcio_schema.SdcioModel_Leaflist{
			Entry: []string{
				"foo",
				"bar",
			},
		},
		Patterntest: ygot.String("foo"),
		NetworkInstance: map[string]*sdcio_schema.SdcioModel_NetworkInstance{
			"default": {
				AdminState:  sdcio_schema.SdcioModelNi_AdminState_disable,
				Description: ygot.String("Default NI"),
				Type:        sdcio_schema.SdcioModelNi_NiType_default,
				Name:        ygot.String("default"),
			},
		},
	}
}

func config2() *sdcio_schema.Device {
	return &sdcio_schema.Device{
		Interface: map[string]*sdcio_schema.SdcioModel_Interface{
			"ethernet-1/2": {
				AdminState:  sdcio_schema.SdcioModelIf_AdminState_enable,
				Description: ygot.String("Foo"),
				Name:        ygot.String("ethernet-1/2"),
				Subinterface: map[uint32]*sdcio_schema.SdcioModel_Interface_Subinterface{
					5: {
						Description: ygot.String("Subinterface 5"),
						Type:        sdcio_schema.SdcioModelCommon_SiType_routed,
						Index:       ygot.Uint32(5),
					},
				},
			},
		},
		Patterntest: ygot.String("bar"),
		NetworkInstance: map[string]*sdcio_schema.SdcioModel_NetworkInstance{
			"other": {
				AdminState:  sdcio_schema.SdcioModelNi_AdminState_enable,
				Description: ygot.String("Other NI"),
				Type:        sdcio_schema.SdcioModelNi_NiType_ip_vrf,
				Name:        ygot.String("other"),
			},
		},
	}

}

func expandUpdateFromConfig(ctx context.Context, conf *sdcio_schema.Device, converter *utils.Converter) ([]*sdcpb.Update, error) {
	if conf == nil {
		return nil, nil
	}

	strJson, err := ygot.EmitJSON(conf, &ygot.EmitJSONConfig{
		Format:         ygot.RFC7951,
		SkipValidation: true,
	})
	if err != nil {
		return nil, err
	}

	return converter.ExpandUpdate(ctx,
		&sdcpb.Update{
			Path: &sdcpb.Path{
				Elem: []*sdcpb.PathElem{},
			},
			Value: &sdcpb.TypedValue{Value: &sdcpb.TypedValue_JsonVal{JsonVal: []byte(strJson)}},
		},
		true)
}

func addToRoot(ctx context.Context, root *RootEntry, updates []*sdcpb.Update, flags *UpdateInsertFlags, owner string, prio int32) error {
	for _, upd := range updates {
		b, err := proto.Marshal(upd.Value)
		if err != nil {
			return err
		}
		cacheUpd := cache.NewUpdate(utils.ToStrings(upd.GetPath(), false, false), b, prio, owner, 0)

		_, err = root.AddCacheUpdateRecursive(ctx, cacheUpd, flags)
		if err != nil {
			return err
		}
	}
	return nil
}
