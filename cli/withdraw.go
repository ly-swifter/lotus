package cli

import (
	"bytes"
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/network"
	api2 "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var ActorWithdrawCmd = &cli.Command{
	Name:      "withdraw",
	Usage:     "withdraw available balance to beneficiary",
	ArgsUsage: "[MinerAddr] [amount (FIL)]",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "confidence",
			Usage: "number of block confirmations to wait for",
			Value: int(build.MessageConfidence),
		},
		&cli.BoolFlag{
			Name:  "beneficiary",
			Usage: "send withdraw message from the beneficiary address",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, acloser, err := GetFullNodeAPIV1(cctx)
		if err != nil {
			return err
		}
		defer acloser()

		ctx := ReqContext(cctx)

		if cctx.Args().Len() != 2 {
			return fmt.Errorf("usage: withdraw <MinerAddr> <Amount (FIL)>")
		}

		maddr, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return err
		}

		f, err := types.ParseFIL(cctx.Args().Get(1))
		if err != nil {
			return xerrors.Errorf("parsing 'amount' argument: %w", err)
		}

		amount := abi.TokenAmount(f)

		var res cid.Cid
		if cctx.IsSet("beneficiary") {
			res, err = api.BeneficiaryWithdrawBalance(ctx, maddr, amount)
		} else {
			res, err = api.ActorWithdrawBalance(ctx, maddr, amount)
		}
		if err != nil {
			return err
		}

		fmt.Printf("Requested withdrawal in message %s\nwaiting for it to be included in a block..\n", res)

		// wait for it to get mined into a block
		wait, err := api.StateWaitMsg(ctx, res, uint64(cctx.Int("confidence")), api2.LookbackNoLimit, true)
		if err != nil {
			return xerrors.Errorf("Timeout waiting for withdrawal message %s", wait.Message)
		}

		if wait.Receipt.ExitCode.IsError() {
			return xerrors.Errorf("Failed to execute withdrawal message %s: %w", wait.Message, wait.Receipt.ExitCode.Error())
		}

		nv, err := api.StateNetworkVersion(ctx, wait.TipSet)
		if err != nil {
			return err
		}

		if nv >= network.Version14 {
			var withdrawn abi.TokenAmount
			if err := withdrawn.UnmarshalCBOR(bytes.NewReader(wait.Receipt.Return)); err != nil {
				return err
			}

			fmt.Printf("Successfully withdrew %s \n", types.FIL(withdrawn))
			if withdrawn.LessThan(amount) {
				fmt.Printf("Note that this is less than the requested amount of %s\n", types.FIL(amount))
			}
		}

		return nil
	},
}
