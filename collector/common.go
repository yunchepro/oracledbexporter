package collector

import (
	"regexp"
	"strconv"
	"strings"

	"io/ioutil"

	"gopkg.in/yaml.v2"
)

var (
	labelRemovePattern = regexp.MustCompile("[:()*/-]")
	labelRemoveDup     = regexp.MustCompile("  +")
)

func formatInList(params []string) string {
	var s []string
	for _, p := range params {
		s = append(s, "'"+p+"'")
	}
	return strings.Join(s, ",")
}

// replace space with _, remove invalid char,
func formatLabel(s string) string {

	ns := labelRemoveDup.ReplaceAllString(labelRemovePattern.ReplaceAllString(s, ""), "_")
	return strings.Replace(strings.ToLower(ns), " ", "_", -1)
}

func formatFloat64(val float64) string {
	return strconv.FormatFloat(val, 'f', 0, 64)
}

func loadContext() (map[string]string, error) {
	var c = make(map[string]string)
	buf, err := ioutil.ReadFile("context.yaml")
	if err != nil {
		// return empty map
		return c, err
	}

	err = yaml.Unmarshal(buf, &c)
	return c, err
}

func saveContext(c map[string]string) error {
	out, err := yaml.Marshal(c)
	ioutil.WriteFile("context.yaml", out, 0666)
	return err
}

func parseVersion(vs string) (float64, error) {
	elems := strings.Split(vs, ".")
	prefix := len(elems)
	if prefix > 2 {
		prefix = 2
	}
	vv := strings.Join(elems[0:prefix], ".")
	return strconv.ParseFloat(vv, 64)
}
