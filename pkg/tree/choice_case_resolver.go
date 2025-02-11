package tree

import (
	"math"
	"slices"
)

type choiceCasesResolvers map[string]*choiceCasesResolver

// AddChoice adds / registers a new Choice to the choiceCasesResolver
func (c choiceCasesResolvers) AddChoice(name string) *choiceCasesResolver {
	r := newChoiceCasesResolver()
	c[name] = r
	return r
}

func (c choiceCasesResolvers) deepCopy() choiceCasesResolvers {
	result := choiceCasesResolvers{}

	for k, v := range c {
		result[k] = v.deepCopy()
	}

	return result

}

// GetSkipElements returns the list of all choices elements that are not highes priority.
// The resulting slice is used to skip these elements.
func (c choiceCasesResolvers) GetSkipElements() []string {
	result := []string{}
	for _, x := range c {
		result = append(result, x.GetSkipElements()...)
	}
	return result
}

func (c choiceCasesResolvers) remainsToExist() bool {
	for _, x := range c {
		if x.getBestCaseName() != "" {
			return true
		}
	}
	return false
}

// GetChoiceElements returns a list of elements that belong to the same choice
// as the given element. This is used to query the cache for all elements of all cases for the
// choice.
func (c choiceCasesResolvers) GetChoiceElementNeighbors(elemName string) []string {
	var result []string
	// iterate through the different choices that might exist
	for _, choice := range c {
		// get all the elements references in a certain contains
		sl := choice.GetElementNames()
		// check for the given element, if it belongs to the actual choice
		var idx int
		if idx = slices.Index(sl, elemName); idx == -1 {
			// if not continue with next choice
			continue
		}
		// if it belongs to it, return the list of all elements belonging to the
		// choice without the provided element itself.
		return append(sl[:idx], sl[idx+1:]...)

	}
	return result
}

// choiceCasesResolver is a helper used to efficiently store the priority values of certain branches and their association to the cases is a choice.
// All with the goal of composing a list of elements that do not belong to the prioritised case, for exclusion on tree traversal time.
type choiceCasesResolver struct {
	cases                map[string]*choicesCase
	elementToCaseMapping map[string]string
}

func (c *choiceCasesResolver) deepCopy() *choiceCasesResolver {
	result := &choiceCasesResolver{
		cases:                map[string]*choicesCase{},
		elementToCaseMapping: map[string]string{},
	}

	for k, v := range c.cases {
		result.cases[k] = v.deepCopy()
	}

	for k, v := range c.elementToCaseMapping {
		result.elementToCaseMapping[k] = v
	}

	return result
}

// GetElementNames retrieve all the Element names involved in the Choice
func (c *choiceCasesResolver) GetElementNames() []string {
	result := make([]string, 0, len(c.cases))
	for elemName := range c.elementToCaseMapping {
		result = append(result, elemName)
	}
	return result
}

// choicesCase is the representation of a case in the choiceCasesResolver.
type choicesCase struct {
	name     string
	elements map[string]*choicesCaseElement
}

func (c *choicesCase) deepCopy() *choicesCase {
	result := &choicesCase{
		name: c.name,
	}
	for k, v := range c.elements {
		result.elements[k] = v.deepCopy()
	}
	return result
}

func (c *choicesCase) GetLowestPriorityValue() int32 {
	result := int32(math.MaxInt32)
	for _, cas := range c.elements {
		if cas.value < result {
			result = cas.value
		}
	}
	return result
}

func (c *choicesCase) GetLowestPriorityValueOld() int32 {
	result := int32(math.MaxInt32)
	for _, cas := range c.elements {
		if !cas.new && cas.value < result {
			result = cas.value
		}
	}
	return result
}

type choicesCaseElement struct {
	name  string
	value int32
	new   bool
}

func (c *choicesCaseElement) deepCopy() *choicesCaseElement {
	return &choicesCaseElement{
		name:  c.name,
		value: c.value,
		new:   c.new,
	}
}

// newChoiceCasesResolver returns a ready to use choiceCasesResolver.
func newChoiceCasesResolver() *choiceCasesResolver {
	return &choiceCasesResolver{
		cases:                map[string]*choicesCase{}, // case name -> case data
		elementToCaseMapping: map[string]string{},       // element name -> case name
	}
}

// AddCase adds / registers a case with its name and the element names that belong to the case
func (c *choiceCasesResolver) AddCase(name string, elements []string) *choicesCase {
	c.cases[name] = &choicesCase{
		name:     name,
		elements: map[string]*choicesCaseElement{},
	}
	for _, e := range elements {
		c.elementToCaseMapping[e] = name
		c.cases[name].elements[e] = &choicesCaseElement{
			name:  e,
			value: int32(math.MaxInt32),
		}
	}
	return c.cases[name]
}

// SetValue Sets the priority value that the given elements with its entire branch has calculated
func (c *choiceCasesResolver) SetValue(elemName string, v int32, new bool) {
	// math.MaxInt32 indicates that the branch is not populated,
	// so we skip adding it
	if v == math.MaxInt32 {
		return
	}
	actualCase := c.elementToCaseMapping[elemName]
	c.cases[actualCase].elements[elemName].value = v
	c.cases[actualCase].elements[elemName].new = new
}

// GetBestCaseName returns the name of the case, that has the highes priority
func (c *choiceCasesResolver) getBestCaseName() string {
	var bestCaseName string
	bestCasePrio := int32(math.MaxInt32)
	for caseName, cas := range c.cases {
		if cas.GetLowestPriorityValue() < bestCasePrio {
			bestCaseName = caseName
			bestCasePrio = cas.GetLowestPriorityValue()
		}
	}
	return bestCaseName
}

func (c *choiceCasesResolver) getOldBestCaseName() string {
	var bestCaseName string
	bestCasePrio := int32(math.MaxInt32)
	for caseName, cas := range c.cases {
		lowestPrioOld := cas.GetLowestPriorityValueOld()
		if lowestPrioOld < bestCasePrio {
			bestCaseName = caseName
			bestCasePrio = lowestPrioOld
		}
	}
	return bestCaseName
}

// GetSkipElements returns the names of all the elements that belong to
// cases that have not the best priority
func (c *choiceCasesResolver) GetSkipElements() []string {
	result := make([]string, 0, len(c.elementToCaseMapping))

	bestCase := c.getBestCaseName()

	for elem, cas := range c.elementToCaseMapping {
		if cas == bestCase {
			continue
		}
		result = append(result, elem)
	}
	return result
}
