package main

import (
	"fmt"

	"io/ioutil"

	"gopkg.in/yaml.v2"
)

var (
	configFile = "config.yaml"
)

type Config struct {
	Name string
}

func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain Config
	fmt.Printf("Before UnmarshaYaml, config: %s\n", c)
	err := unmarshal((*plain)(c))
	if err != nil {
		fmt.Printf("unmarshal error: %s\n", err)
		return err
	}
	fmt.Printf("After UnmarshaYaml, config: %s\n", c)
	return nil
}

func main() {
	buf, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Printf("Read file error: %s", err)
		return
	}

	c := Config{}
	yaml.Unmarshal(buf, &c)

}
