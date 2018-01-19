package testserver

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/lomik/go-carbon/carbon"
	"github.com/lomik/zapwriter"
)

type Carbon struct {
	RootDir          string
	TcpPort          int
	PicklePort       int
	CarbonserverPort int
	Schemas          []SchemaConfig
	Aggregations     []AggregationConfig

	app *carbon.App
	cmd *exec.Cmd
}

func (s *Carbon) Start() error {
	err := s.setup()
	if err != nil {
		return err
	}
	s.app = carbon.New(s.CarbonConfigFilename())
	err = s.app.ParseConfig()
	if err != nil {
		return err
	}
	return s.startProcess()
}

func (s *Carbon) startProcess() error {
	const execFilename = "go-carbon"
	path, err := exec.LookPath(execFilename)
	if err != nil {
		return fmt.Errorf("executable %q not found in $PATH", execFilename)
	}
	s.cmd = exec.Command(path, "-config", s.CarbonConfigFilename())
	err = s.cmd.Start()
	if err != nil {
		return err
	}
	return nil
}

func (s *Carbon) CarbonConfigFilename() string {
	return filepath.Join(s.RootDir, "go-carbon.conf")
}

func (s *Carbon) dataDirname() string {
	return filepath.Join(s.RootDir, "data")
}

func (s *Carbon) schemasFilename() string {
	return filepath.Join(s.RootDir, "storage-schemas.conf")
}

func (s *Carbon) aggregationFilename() string {
	return filepath.Join(s.RootDir, "storage-aggregation.conf")
}

func (s *Carbon) logDirname() string {
	return s.RootDir
}

func (s *Carbon) writeCarbonConfigFile() error {
	u, err := user.Current()
	if err != nil {
		return err
	}
	cfg := NewConfig()
	cfg.Common.User = u.Username
	cfg.Udp.Enabled = false
	cfg.Grpc.Enabled = false
	cfg.Carbonlink.Enabled = false
	cfg.Whisper.DataDir = s.dataDirname()
	cfg.Whisper.SchemasFilename = s.schemasFilename()
	cfg.Whisper.AggregationFilename = s.aggregationFilename()
	cfg.Logging = []zapwriter.Config{
		{
			File:             filepath.Join(s.logDirname(), "go-carbon.log"),
			Level:            "info",
			Encoding:         "console",
			EncodingTime:     "millis",
			EncodingDuration: "string",
		},
	}

	if s.TcpPort != 0 {
		cfg.Tcp.Listen = fmt.Sprintf("127.0.0.1:%d", s.TcpPort)
		cfg.Tcp.Enabled = true
	} else {
		cfg.Tcp.Enabled = false
	}
	if s.PicklePort != 0 {
		cfg.Pickle.Listen = fmt.Sprintf("127.0.0.1:%d", s.PicklePort)
		cfg.Pickle.Enabled = true
	} else {
		cfg.Pickle.Enabled = false
	}
	if s.CarbonserverPort != 0 {
		cfg.Carbonserver.Listen = fmt.Sprintf("127.0.0.1:%d", s.CarbonserverPort)
		cfg.Carbonserver.Enabled = true
	} else {
		cfg.Carbonserver.Enabled = false
	}

	file, err := os.Create(s.CarbonConfigFilename())
	if err != nil {
		return err
	}
	defer file.Close()

	enc := toml.NewEncoder(file)
	enc.Indent = ""
	err = enc.Encode(cfg)
	if err != nil {
		return err
	}
	return nil
}

func (s *Carbon) setup() error {
	err := os.MkdirAll(s.dataDirname(), 0700)
	if err != nil {
		return err
	}
	err = s.writeCarbonConfigFile()
	if err != nil {
		return err
	}
	err = schemasConfig(s.Schemas).writeFile(s.schemasFilename())
	if err != nil {
		return err
	}
	err = aggregationsConfig(s.Aggregations).writeFile(s.aggregationFilename())
	if err != nil {
		return err
	}
	return nil
}

func (s *Carbon) Wait() error {
	return s.cmd.Wait()
}

func (s *Carbon) Kill() error {
	return s.cmd.Process.Kill()
}
