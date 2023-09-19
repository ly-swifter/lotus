package main

import (
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/repo"
)

var initCmd = &cli.Command{
	Name:   "init",
	Usage:  "Initial offline wallet",
	Hidden: true,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "repo",
			EnvVars: []string{"OFFLINE_WALLET_PATH"},
			Hidden:  true,
			Value:   "~/.offline-wallet", // TODO: Consider XDG_DATA_HOME
		},
		&cli.BoolFlag{
			Name:  "enabled",
			Usage: "Initial offline wallet",
			Value: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		// fmt.Println("DEPRECATED: This command will be removed in the future")

		lr, _, err := openRepo(cctx)
		if err != nil {
			return err
		}

		lr.Close()

		return nil
	},
}

func openRepo(cctx *cli.Context) (repo.LockedRepo, types.KeyStore, error) {
	repopath := cctx.String("repo")
	r, err := repo.NewFS(repopath)
	if err != nil {
		return nil, nil, err
	}

	ok, err := r.Exists()
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		if err := r.Init(repo.Wallet); err != nil {
			return nil, nil, err
		}
	}

	lr, err := r.Lock(repo.Wallet)
	if err != nil {
		return nil, nil, err
	}

	ks, err := lr.KeyStore()
	if err != nil {
		return nil, nil, err
	}

	return lr, ks, nil
}
