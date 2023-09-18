package impl

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	builtintypes "github.com/filecoin-project/go-state-types/builtin"
	minertypes "github.com/filecoin-project/go-state-types/builtin/v9/miner"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/impl/client"
	"github.com/filecoin-project/lotus/node/impl/common"
	"github.com/filecoin-project/lotus/node/impl/full"
	"github.com/filecoin-project/lotus/node/impl/market"
	"github.com/filecoin-project/lotus/node/impl/net"
	"github.com/filecoin-project/lotus/node/impl/paych"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/lp2p"
)

var log = logging.Logger("node")

type FullNodeAPI struct {
	common.CommonAPI
	net.NetAPI
	full.ChainAPI
	client.API
	full.MpoolAPI
	full.GasAPI
	market.MarketAPI
	paych.PaychAPI
	full.StateAPI
	full.MsigAPI
	full.WalletAPI
	full.SyncAPI
	full.RaftAPI
	full.EthAPI

	DS          dtypes.MetadataDS
	NetworkName dtypes.NetworkName
}

func (a *FullNodeAPI) ActorWithdrawBalance(ctx context.Context, maddr address.Address, amount abi.TokenAmount) (cid.Cid, error) {
	return a.withdrawBalance(ctx, maddr, amount, true)
}

func (a *FullNodeAPI) BeneficiaryWithdrawBalance(ctx context.Context, maddr address.Address, amount abi.TokenAmount) (cid.Cid, error) {
	return a.withdrawBalance(ctx, maddr, amount, false)
}

func (a *FullNodeAPI) withdrawBalance(ctx context.Context, maddr address.Address, amount abi.TokenAmount, fromOwner bool) (cid.Cid, error) {
	available, err := a.StateMinerAvailableBalance(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return cid.Undef, xerrors.Errorf("Error getting miner balance: %w", err)
	}

	if amount.GreaterThan(available) {
		return cid.Undef, xerrors.Errorf("can't withdraw more funds than available; requested: %s; available: %s", types.FIL(amount), types.FIL(available))
	}

	if amount.Equals(big.Zero()) {
		amount = available
	}

	params, err := actors.SerializeParams(&minertypes.WithdrawBalanceParams{
		AmountRequested: amount,
	})
	if err != nil {
		return cid.Undef, err
	}

	mi, err := a.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return cid.Undef, xerrors.Errorf("Error getting miner's owner address: %w", err)
	}

	var sender address.Address
	if fromOwner {
		sender = mi.Owner
	} else {
		sender = mi.Beneficiary
	}

	smsg, err := a.MpoolPushMessage(ctx, &types.Message{
		To:     maddr,
		From:   sender,
		Value:  types.NewInt(0),
		Method: builtintypes.MethodsMiner.WithdrawBalance,
		Params: params,
	}, nil)
	if err != nil {
		return cid.Undef, err
	}

	return smsg.Cid(), nil
}

func (n *FullNodeAPI) CreateBackup(ctx context.Context, fpath string) error {
	return backup(ctx, n.DS, fpath)
}

func (n *FullNodeAPI) NodeStatus(ctx context.Context, inclChainStatus bool) (status api.NodeStatus, err error) {
	curTs, err := n.ChainHead(ctx)
	if err != nil {
		return status, err
	}

	status.SyncStatus.Epoch = uint64(curTs.Height())
	timestamp := time.Unix(int64(curTs.MinTimestamp()), 0)
	delta := time.Since(timestamp).Seconds()
	status.SyncStatus.Behind = uint64(delta / 30)

	// get peers in the messages and blocks topics
	peersMsgs := make(map[peer.ID]struct{})
	peersBlocks := make(map[peer.ID]struct{})

	for _, p := range n.PubSub.ListPeers(build.MessagesTopic(n.NetworkName)) {
		peersMsgs[p] = struct{}{}
	}

	for _, p := range n.PubSub.ListPeers(build.BlocksTopic(n.NetworkName)) {
		peersBlocks[p] = struct{}{}
	}

	// get scores for all connected and recent peers
	scores, err := n.NetPubsubScores(ctx)
	if err != nil {
		return status, err
	}

	for _, score := range scores {
		if score.Score.Score > lp2p.PublishScoreThreshold {
			_, inMsgs := peersMsgs[score.ID]
			if inMsgs {
				status.PeerStatus.PeersToPublishMsgs++
			}

			_, inBlocks := peersBlocks[score.ID]
			if inBlocks {
				status.PeerStatus.PeersToPublishBlocks++
			}
		}
	}

	if inclChainStatus && status.SyncStatus.Epoch > uint64(build.Finality) {
		blockCnt := 0
		ts := curTs

		for i := 0; i < 100; i++ {
			blockCnt += len(ts.Blocks())
			tsk := ts.Parents()
			ts, err = n.ChainGetTipSet(ctx, tsk)
			if err != nil {
				return status, err
			}
		}

		status.ChainStatus.BlocksPerTipsetLast100 = float64(blockCnt) / 100

		for i := 100; i < int(build.Finality); i++ {
			blockCnt += len(ts.Blocks())
			tsk := ts.Parents()
			ts, err = n.ChainGetTipSet(ctx, tsk)
			if err != nil {
				return status, err
			}
		}

		status.ChainStatus.BlocksPerTipsetLastFinality = float64(blockCnt) / float64(build.Finality)

	}

	return status, nil
}

func (n *FullNodeAPI) RaftState(ctx context.Context) (*api.RaftStateData, error) {
	return n.RaftAPI.GetRaftState(ctx)
}

func (n *FullNodeAPI) RaftLeader(ctx context.Context) (peer.ID, error) {
	return n.RaftAPI.Leader(ctx)
}

var _ api.FullNode = &FullNodeAPI{}
