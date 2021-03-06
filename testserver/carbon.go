package testserver

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

type Carbon struct {
	RootDir            string
	TCPListen          string
	ProtobufListen     string
	CarbonserverListen string
	Schemas            []SchemaConfig
	Aggregations       []AggregationConfig

	cmd *exec.Cmd
}

func (c *Carbon) Start() error {
	err := c.setup()
	if err != nil {
		return err
	}
	return c.startProcess()
}

func (c *Carbon) startProcess() error {
	const execFilename = "go-carbon"
	path, err := exec.LookPath(execFilename)
	if err != nil {
		return fmt.Errorf("executable %q not found in $PATH", execFilename)
	}
	c.cmd = exec.Command(path, "-config", c.CarbonConfigFilename())
	err = c.cmd.Start()
	if err != nil {
		return err
	}
	return nil
}

func (c *Carbon) CarbonConfigFilename() string {
	return filepath.Join(c.RootDir, "go-carbon.conf")
}

func (c *Carbon) dataDirname() string {
	return filepath.Join(c.RootDir, "data")
}

func (c *Carbon) schemasFilename() string {
	return filepath.Join(c.RootDir, "storage-schemas.conf")
}

func (c *Carbon) aggregationFilename() string {
	return filepath.Join(c.RootDir, "storage-aggregation.conf")
}

func (c *Carbon) logDirname() string {
	return c.RootDir
}

func (c *Carbon) setup() error {
	err := os.MkdirAll(c.dataDirname(), 0700)
	if err != nil {
		return err
	}
	err = c.writeCarbonConfigFile(c.CarbonConfigFilename())
	if err != nil {
		return err
	}

	err = schemasConfig(c.Schemas).writeFile(c.schemasFilename())
	if err != nil {
		return err
	}
	err = aggregationsConfig(c.Aggregations).writeFile(c.aggregationFilename())
	if err != nil {
		return err
	}
	return nil
}

func (c *Carbon) Wait() error {
	return c.cmd.Wait()
}

func (c *Carbon) Kill() error {
	return c.cmd.Process.Kill()
}
