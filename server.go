package carbontest

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/lomik/go-carbon/carbon"
)

type Server struct {
	RootDir          string
	TcpPort          int
	PicklePort       int
	CarbonserverPort int
	Schemas          SchemasConfig
	Aggregation      AggregationsConfig

	app *carbon.App
}

func (s *Server) Start() error {
	err := s.setup()
	if err != nil {
		return err
	}
	s.app = carbon.New(s.CarbonConfigFilename())
	err = s.app.ParseConfig()
	if err != nil {
		return err
	}
	return s.app.Start()
}

func (s *Server) CarbonConfigFilename() string {
	return filepath.Join(s.RootDir, "go-carbon.conf")
}

func (s *Server) dataDirname() string {
	return filepath.Join(s.RootDir, "data")
}

func (s *Server) schemasFilename() string {
	return filepath.Join(s.RootDir, "storage-schemas.conf")
}

func (s *Server) aggregationFilename() string {
	return filepath.Join(s.RootDir, "storage-aggregation.conf")
}

func (s *Server) writeCarbonConfigFile() error {
	cfg := carbon.NewConfig()
	cfg.Udp.Enabled = false
	cfg.Grpc.Enabled = false
	cfg.Carbonlink.Enabled = false
	cfg.Whisper.DataDir = s.dataDirname()
	cfg.Whisper.SchemasFilename = s.schemasFilename()
	cfg.Whisper.AggregationFilename = s.aggregationFilename()

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

func (s *Server) setup() error {
	err := os.MkdirAll(s.dataDirname(), 0700)
	if err != nil {
		return err
	}
	err = s.writeCarbonConfigFile()
	if err != nil {
		return err
	}
	err = s.Schemas.WriteFile(s.schemasFilename())
	if err != nil {
		return err
	}
	err = s.Aggregation.WriteFile(s.aggregationFilename())
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) Loop() {
	s.app.Loop()
}

func (s *Server) ForceStop() {
	s.app.Stop()
}

func (s *Server) GracefulStop() error {
	return s.app.DumpStop()
}
