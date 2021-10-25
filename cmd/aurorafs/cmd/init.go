package cmd

import (
	"fmt"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/node"
	"github.com/spf13/cobra"
)

func (c *command) initInitCmd() (err error) {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialise a aufs node",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) > 0 {
				return cmd.Help()
			}

			v := strings.ToLower(c.config.GetString(optionNameVerbosity))
			logger, err := newLogger(cmd, v)
			if err != nil {
				return fmt.Errorf("new logger: %v", err)
			}

			signerConfig, err := c.configureSigner(cmd, logger)
			if err != nil {
				return err
			}

			dataDir := c.config.GetString(optionNameDataDir)
			stateStore, err := node.InitStateStore(logger, dataDir)
			if err != nil {
				return err
			}

			defer stateStore.Close()

			return node.CheckOverlayWithStore(signerConfig.address, stateStore)
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return c.config.BindPFlags(cmd.Flags())
		},
	}

	c.setAllFlags(cmd)
	c.root.AddCommand(cmd)
	return nil
}
