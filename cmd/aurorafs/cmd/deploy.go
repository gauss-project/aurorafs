package cmd

import (
	"fmt"
	"strings"

	"github.com/gauss-project/aurorafs/pkg/node"
	"github.com/spf13/cobra"
)

func (c *command) initDeployCmd() error {
	cmd := &cobra.Command{
		Use:   "deploy",
		Short: "Deploy and fund the chequebook contract",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if (len(args)) > 0 {
				return cmd.Help()
			}

			v := strings.ToLower(c.config.GetString(optionNameVerbosity))
			logger, err := newLogger(cmd, v)
			if err != nil {
				return fmt.Errorf("new logger: %v", err)
			}

			dataDir := c.config.GetString(optionNameDataDir)
			//factoryAddress := c.config.GetString(optionNameSwapFactoryAddress)
			//swapInitialDeposit := c.config.GetString(optionNameSwapInitialDeposit)
			//swapEndpoint := c.config.GetString(optionNameSwapEndpoint)

			stateStore, err := node.InitStateStore(logger, dataDir)
			if err != nil {
				return err
			}

			defer stateStore.Close()

			signerConfig, err := c.configureSigner(cmd, logger)
			if err != nil {
				return err
			}
			//signer := signerConfig.signer

			err = node.CheckOverlayWithStore(signerConfig.address, stateStore)
			if err != nil {
				return err
			}

			//ctx := cmd.Context()

			//swapBackend, overlayEthAddress, chainID, transactionService, err := node.InitChain(
			//	ctx,
			//	logger,
			//	stateStore,
			//	swapEndpoint,
			//	signer,
			//)
			//if err != nil {
			//	return err
			//}
			//defer swapBackend.Close()
			//
			//chequebookFactory, err := node.InitChequebookFactory(
			//	logger,
			//	swapBackend,
			//	chainID,
			//	transactionService,
			//	factoryAddress,
			//)
			//if err != nil {
			//	return err
			//}

			//_, err = node.InitChequebookService(
			//	ctx,
			//	logger,
			//	stateStore,
			//	signer,
			//	chainID,
			//	swapBackend,
			//	overlayEthAddress,
			//	transactionService,
			//	chequebookFactory,
			//	swapInitialDeposit,
			//)

			return err
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return c.config.BindPFlags(cmd.Flags())
		},
	}

	c.setAllFlags(cmd)
	c.root.AddCommand(cmd)

	return nil
}
