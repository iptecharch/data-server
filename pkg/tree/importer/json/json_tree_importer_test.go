package json

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/sdcio/data-server/mocks/mockcacheclient"
	"github.com/sdcio/data-server/pkg/tree"
	"github.com/sdcio/data-server/pkg/utils/testhelper"
	"go.uber.org/mock/gomock"
)

func TestJsonTreeImporter(t *testing.T) {

	tests := []struct {
		name  string
		input string
		ietf  bool
	}{
		{
			name: "JSON",
			input: `
				{
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
			"leaflist": {
				"entry": [
				"foo",
				"bar"
				]
			},
			"network-instance": [
				{
				"admin-state": "enable",
				"description": "Other NI",
				"name": "other",
				"type": "ip-vrf",
				"protocol":{
					"bgp": {}
				}
				}
			],
			"patterntest": "hallo DU",
			"emptyconf": {}
			}`,
		},
		{
			name: "JSON_IETF",
			ietf: true,
			input: `{
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
	}

	// create a gomock controller
	controller := gomock.NewController(t)

	// create a cache client mock
	cacheClient := mockcacheclient.NewMockClient(controller)
	testhelper.ConfigureCacheClientMock(t, cacheClient, nil, nil, nil, nil)

	dsName := "dev1"
	scb, err := testhelper.GetSchemaClientBound(t, controller)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := tree.NewTreeContext(tree.NewTreeSchemaCacheClient(dsName, cacheClient, scb), "test")
			root, err := tree.NewTreeRoot(ctx, tc)
			if err != nil {
				t.Error(err)
			}

			jsonBytes := []byte(tt.input)

			var j any
			err = json.Unmarshal(jsonBytes, &j)
			if err != nil {
				t.Fatalf("error parsing json document: %v", err)
			}
			jti := NewJsonTreeImporter(j)
			err = root.ImportConfig(ctx, jti, tree.RunningIntentName, tree.RunningValuesPrio)
			if err != nil {
				t.Fatal(err)
			}

			root.FinishInsertionPhase(ctx)
			t.Log(root.String())

			var result any
			if tt.ietf {
				result, err = root.ToJsonIETF(false)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				result, err = root.ToJson(false)
				if err != nil {
					t.Fatal(err)
				}
			}

			// we need to marshal and then unmarshall again,
			// since it is not clear if a certain value is float64 or uint64...
			// so we do the conversion back and forth again, such that the json package can take the same type desisions
			bresult, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			err = json.Unmarshal(bresult, &result)
			if err != nil {
				t.Fatalf("error parsing json document: %v", err)
			}
			t.Log(string(bresult))

			if diff := cmp.Diff(j, result); diff != "" {
				t.Errorf("Error imported data differs:%s", diff)
			}
		})
	}
}
