package main

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"github.com/filecoin-project/go-state-types/builtin/v8/market"
	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"html/template"
	"io"
	"mime"
	"net"
	"net/http"
	gopath "path"
	"sort"
	"strconv"
	"strings"
	"sync"
	txtempl "text/template"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"github.com/gorilla/mux"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	io2 "github.com/ipfs/go-unixfs/io"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/node/repo"
)

//go:embed dexpl
var dres embed.FS

var maxDirTypeChecks, typeCheckDepth int64 = 16, 15

var dataexplCmd = &cli.Command{
	Name:  "run",
	Usage: "Explore data stored on filecoin",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := lcli.GetFullNodeAPIV1(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		ainfo, err := lcli.GetAPIInfo(cctx, repo.FullNode)
		if err != nil {
			return xerrors.Errorf("could not get API info: %w", err)
		}

		mpcs, err := api.StateMarketParticipants(ctx, types.EmptyTSK)
		if err != nil {
			return xerrors.Errorf("getting market participants: %w", err)
		}

		type mminer struct {
			Addr   address.Address
			Locked types.FIL
		}
		var mminers []mminer

		for sa, mb := range mpcs {
			if mb.Locked.IsZero() {
				continue
			}

			a, err := address.NewFromString(sa)
			if err != nil {
				return err
			}

			ac, err := api.StateGetActor(ctx, a, types.EmptyTSK)
			if err != nil {
				return err
			}

			if builtin.IsStorageMinerActor(ac.Code) {
				mminers = append(mminers, mminer{
					Addr:   a,
					Locked: types.FIL(mb.Locked),
				})
			}
		}
		sort.Slice(mminers, func(i, j int) bool {
			return big.Cmp(abi.TokenAmount(mminers[i].Locked), abi.TokenAmount(mminers[j].Locked)) > 0
		})

		m := mux.NewRouter()

		m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			tpl, err := template.ParseFS(dres, "dexpl/index.gohtml")
			if err != nil {
				fmt.Println(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			data := map[string]interface{}{}
			if err := tpl.Execute(w, data); err != nil {
				fmt.Println(err)
				return
			}

		}).Methods("GET")

		m.HandleFunc("/miners", func(w http.ResponseWriter, r *http.Request) {
			tpl, err := template.ParseFS(dres, "dexpl/miners.gohtml")
			if err != nil {
				fmt.Println(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			data := map[string]interface{}{
				"miners": mminers,
			}
			if err := tpl.Execute(w, data); err != nil {
				fmt.Println(err)
				return
			}

		}).Methods("GET")

		m.HandleFunc("/ping/miner/{id}", func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			vars := mux.Vars(r)
			a, err := address.NewFromString(vars["id"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			mi, err := api.StateMinerInfo(ctx, a, types.EmptyTSK)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if mi.PeerId == nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			multiaddrs := make([]multiaddr.Multiaddr, 0, len(mi.Multiaddrs))
			for i, a := range mi.Multiaddrs {
				maddr, err := multiaddr.NewMultiaddrBytes(a)
				if err != nil {
					log.Warnf("parsing multiaddr %d (%x): %s", i, a, err)
					continue
				}
				multiaddrs = append(multiaddrs, maddr)
			}

			pi := peer.AddrInfo{
				ID:    *mi.PeerId,
				Addrs: multiaddrs,
			}

			{
				ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
				defer cancel()

				if err := api.NetConnect(ctx, pi); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}

			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			d, err := api.NetPing(ctx, pi.ID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(fmt.Sprintf("%s %s", pi.ID, d.Round(time.Millisecond))))
		}).Methods("GET")

		m.HandleFunc("/deals", func(w http.ResponseWriter, r *http.Request) {
			deals, err := api.ClientListDeals(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			tpl, err := template.ParseFS(dres, "dexpl/client_deals.gohtml")
			if err != nil {
				fmt.Println(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			data := map[string]interface{}{
				"deals":             deals,
				"StorageDealActive": storagemarket.StorageDealActive,
			}
			if err := tpl.Execute(w, data); err != nil {
				fmt.Println(err)
				return
			}

		}).Methods("GET")

		m.HandleFunc("/minersectors/{id}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			ma, err := address.NewFromString(vars["id"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			ms, err := api.StateMinerSectors(ctx, ma, nil, types.EmptyTSK)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			var deals []abi.DealID
			for _, info := range ms {
				for _, d := range info.DealIDs {
					deals = append(deals, d)
				}
			}

			ctx := r.Context()

			commps := map[abi.DealID]cid.Cid{}
			var wg sync.WaitGroup
			wg.Add(len(deals))
			var lk sync.Mutex

			for _, deal := range deals {
				go func(deal abi.DealID) {
					defer wg.Done()

					md, err := api.StateMarketStorageDeal(ctx, deal, types.EmptyTSK)
					if err != nil {
						return
					}

					lk.Lock()
					commps[deal] = md.Proposal.PieceCID
					lk.Unlock()
				}(deal)
			}
			wg.Wait()

			tpl, err := template.ParseFS(dres, "dexpl/sectors.gohtml")
			if err != nil {
				fmt.Println(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			data := map[string]interface{}{
				"maddr":   ma,
				"sectors": ms,
				"deals":   commps,
			}
			if err := tpl.Execute(w, data); err != nil {
				fmt.Println(err)
				return
			}

		}).Methods("GET")

		m.HandleFunc("/deal/{id}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			did, err := strconv.ParseInt(vars["id"], 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			d, err := api.StateMarketStorageDeal(ctx, abi.DealID(did), types.EmptyTSK)
			if err != nil {
				http.Error(w, xerrors.Errorf("StateMarketStorageDeal: %w", err).Error(), http.StatusInternalServerError)
				return
			}

			lstr, err := d.Proposal.Label.ToString()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			dcid, err := cid.Parse(lstr)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			lstr = dcid.String()
			d.Proposal.Label, _ = market.NewLabelFromString(lstr) // if it's b64, will break urls

			var typ string
			var sz string
			var linkc int
			var dents []dirEntry

			{
				ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
				g := get(ainfo, api, r, d.Proposal.Provider, d.Proposal.PieceCID, dcid)
				root, dserv, err := g(ssb.ExploreRecursive(selector.RecursionLimitDepth(typeCheckDepth),
					ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
						eb.Insert("Links", ssb.ExploreIndex(0, ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
							eb.Insert("Hash", ssb.ExploreRecursiveEdge())
						})))
					})),
				))
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				node, err := dserv.Get(ctx, root)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				var s uint64

				switch root.Type() {
				case cid.DagProtobuf:
					protoBufNode, ok := node.(*merkledag.ProtoNode)
					if !ok {
						http.Error(w, "not a dir", http.StatusInternalServerError)
						return
					}

					fsNode, err := unixfs.FSNodeFromBytes(protoBufNode.Data())
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}

					s = fsNode.FileSize()

					switch fsNode.Type() {
					case unixfs.TDirectory:
						d, err := io2.NewDirectoryFromNode(dserv, node)
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}

						ls, err := d.Links(ctx)
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}

						if r.FormValue("expand") == "1" {
							dents, err = parseLinks(ctx, ls, node, dserv, 0)
							if err != nil {
								http.Error(w, err.Error(), http.StatusInternalServerError)
								return
							}
						}

						linkc = len(ls)
						typ = "DIR"
					case unixfs.TFile:
						rd, err := io2.NewDagReader(ctx, node, dserv)
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}

						mimeType, err := mimetype.DetectReader(rd)
						if err != nil {
							http.Error(w, fmt.Sprintf("cannot detect content-type: %s", err.Error()), http.StatusInternalServerError)
							return
						}

						typ = fmt.Sprintf("FILE(%s)", mimeType)

					default:
						http.Error(w, "unknown ufs type "+fmt.Sprint(fsNode.Type()), http.StatusInternalServerError)
						return
					}
				case cid.Raw:
					typ = "FILE"
					mimeType, err := mimetype.DetectReader(bytes.NewReader(node.RawData()))
					if err != nil {
						http.Error(w, fmt.Sprintf("cannot detect content-type: %s", err.Error()), http.StatusInternalServerError)
						return
					}

					typ = fmt.Sprintf("FILE(%s)", mimeType)
					s = uint64(len(node.RawData()))
				case cid.DagCBOR:
					var i interface{}
					err := cbor.DecodeInto(node.RawData(), &i)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}

					typ = fmt.Sprintf("DAG-CBOR")
					s = uint64(len(node.RawData()))
				default:
					http.Error(w, "unknown codec "+fmt.Sprint(root.Type()), http.StatusInternalServerError)
					return
				}

				sz = types.SizeStr(types.NewInt(s))
			}

			now, err := api.ChainHead(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			tpl, err := template.New("deal.gohtml").Funcs(map[string]interface{}{
				"EpochTime": func(e abi.ChainEpoch) string {
					return lcli.EpochTime(now.Height(), e)
				},
				"SizeStr": func(s abi.PaddedPieceSize) string {
					return types.SizeStr(types.NewInt(uint64(s)))
				},
			}).ParseFS(dres, "dexpl/deal.gohtml")
			if err != nil {
				fmt.Println(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			data := map[string]interface{}{
				"deal":  d,
				"label": lstr,
				"id":    did,

				"type":  typ,
				"size":  sz,
				"links": linkc,

				"dirents": dents,
			}
			if err := tpl.Execute(w, data); err != nil {
				fmt.Println(err)
				return
			}

		}).Methods("GET")

		m.HandleFunc("/view/{mid}/{piece}/{cid}/{path:.*}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			ma, err := address.NewFromString(vars["mid"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			pcid, err := cid.Parse(vars["piece"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			dcid, err := cid.Parse(vars["cid"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// retr root
			g := get(ainfo, api, r, ma, pcid, dcid)

			ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
			sel := ssb.Matcher()

			if r.Method != "HEAD" {
				ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

				sel = ssb.ExploreUnion(
					ssb.Matcher(),
					//ssb.ExploreInterpretAs("unixfs", ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.Matcher()))),

					ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
						eb.Insert("Links", ssb.ExploreRange(0, maxDirTypeChecks,
							ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
								eb.Insert("Hash", ssb.ExploreRecursive(selector.RecursionLimitDepth(typeCheckDepth),
									ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
										eb.Insert("Links", ssb.ExploreIndex(0, ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
											eb.Insert("Hash", ssb.ExploreRecursiveEdge())
										})))
									})),
								))
							}),
						))
					}),
				)
			}

			root, dserv, err := g(sel)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			node, err := dserv.Get(ctx, root)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			var rd interface {
				io.ReadSeeker
			}

			switch root.Type() {
			case cid.DagProtobuf:
				protoBufNode, ok := node.(*merkledag.ProtoNode)
				if !ok {
					http.Error(w, "not a dir", http.StatusInternalServerError)
					return
				}

				fsNode, err := unixfs.FSNodeFromBytes(protoBufNode.Data())
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				carpath := strings.Replace(r.URL.Path, "/view", "/car", 1)

				switch fsNode.Type() {
				case unixfs.THAMTShard:
					w.Header().Set("X-HumanSize", types.SizeStr(types.NewInt(must(node.Size))))

					// TODO This in principle should be in the case below, and just use NewDirectoryFromNode; but that doesn't work
					// non-working example: http://127.0.0.1:5658/view/f01872811/baga6ea4seaqej7xagp2cmxdclpzqspd7zmu6dpnjt3qr2tywdzovqosmlvhxemi/bafybeif7kzcu2ezkkr34zu562kvutrsypr5o3rjeqrwgukellocbidfcsu/Links/3/Hash/Links/0/Hash/?filename=baf...qhgi
					ls := node.Links()

					links, err := parseLinks(ctx, ls, node, dserv, maxDirTypeChecks)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}

					tpl, err := template.New("dir.gohtml").ParseFS(dres, "dexpl/dir.gohtml")
					if err != nil {
						fmt.Println(err)
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
					w.Header().Set("Content-Type", "text/html")
					w.WriteHeader(http.StatusOK)
					data := map[string]interface{}{
						"entries": links,
						"url":     r.URL.Path,
						"carurl":  carpath,
					}
					if err := tpl.Execute(w, data); err != nil {
						fmt.Println(err)
						return
					}

					return
				case unixfs.TDirectory:
					w.Header().Set("X-HumanSize", types.SizeStr(types.NewInt(must(node.Size))))

					d, err := io2.NewDirectoryFromNode(dserv, node)
					if err != nil {
						http.Error(w, xerrors.Errorf("newdir: %w", err).Error(), http.StatusInternalServerError)
						return
					}

					ls, err := d.Links(ctx)
					if err != nil {
						http.Error(w, xerrors.Errorf("links: %w", err).Error(), http.StatusInternalServerError)
						return
					}

					w.Header().Set("X-Desc", fmt.Sprintf("DIR (%d entries)", len(ls)))

					if r.Method == "HEAD" {
						w.Header().Set("Content-Type", "text/html")
						w.WriteHeader(http.StatusOK)
						return
					}

					links, err := parseLinks(ctx, ls, node, dserv, maxDirTypeChecks)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}

					tpl, err := template.New("dir.gohtml").ParseFS(dres, "dexpl/dir.gohtml")
					if err != nil {
						fmt.Println(err)
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
					w.Header().Set("Content-Type", "text/html")
					w.WriteHeader(http.StatusOK)
					data := map[string]interface{}{
						"entries": links,
						"url":     r.URL.Path,
						"carurl":  carpath,
					}
					if err := tpl.Execute(w, data); err != nil {
						fmt.Println(err)
						return
					}

					return
				case unixfs.TFile:
					w.Header().Set("X-HumanSize", types.SizeStr(types.NewInt(fsNode.FileSize())))

					ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
					if r.Method == "HEAD" {
						rd, err = getHeadReader(ctx, g, func(spec builder.SelectorSpec, ssb builder.SelectorSpecBuilder) builder.SelectorSpec {
							return spec
						})
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
						break
					} else {
						_, dserv, err = g(ssb.ExploreRecursive(selector.RecursionLimitDepth(typeCheckDepth), ssb.ExploreAll(ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreRecursiveEdge()))))
					}
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}

					rd, err = io2.NewDagReader(ctx, node, dserv)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}

				default:
					http.Error(w, "(2) unknown ufs type "+fmt.Sprint(fsNode.Type()), http.StatusInternalServerError)
					return
				}
			case cid.Raw:
				w.Header().Set("X-HumanSize", types.SizeStr(types.NewInt(must(node.Size))))
				rd = bytes.NewReader(node.RawData())

			case cid.DagCBOR:
				w.Header().Set("X-HumanSize", types.SizeStr(types.NewInt(must(node.Size))))

				// not sure what this is for TBH: we also provide ctx in  &traversal.Config{}
				linkContext := ipld.LinkContext{Ctx: ctx}

				// this is what allows us to understand dagpb
				nodePrototypeChooser := dagpb.AddSupportToChooser(
					func(ipld.Link, ipld.LinkContext) (ipld.NodePrototype, error) {
						return basicnode.Prototype.Any, nil
					},
				)

				// this is how we implement GETs
				linkSystem := cidlink.DefaultLinkSystem()
				linkSystem.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
					cl, isCid := lnk.(cidlink.Link)
					if !isCid {
						return nil, fmt.Errorf("unexpected link type %#v", lnk)
					}

					if node.Cid() != cl.Cid {
						return nil, fmt.Errorf("not found")
					}

					return bytes.NewBuffer(node.RawData()), nil
				}
				unixfsnode.AddUnixFSReificationToLinkSystem(&linkSystem)

				// this is how we pull the start node out of the DS
				startLink := cidlink.Link{Cid: node.Cid()}
				startNodePrototype, err := nodePrototypeChooser(startLink, linkContext)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				startNode, err := linkSystem.Load(
					linkContext,
					startLink,
					startNodePrototype,
				)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				var dumpNode func(n ipld.Node, p string) (string, error)
				dumpNode = func(n ipld.Node, recPath string) (string, error) {
					switch n.Kind() {
					case datamodel.Kind_Invalid:
						return `<span class="node">INVALID</span>`, nil
					case datamodel.Kind_Map:
						var inner string

						it := n.MapIterator()
						for !it.Done() {
							k, v, err := it.Next()
							if err != nil {
								return "", err
							}

							ks, err := dumpNode(k, recPath)
							if err != nil {
								return "", err
							}
							vs, err := dumpNode(v, recPath+must(k.AsString)+"/")
							if err != nil {
								return "", err
							}

							inner += fmt.Sprintf("<tr><td>%s</td><td>%s</td></tr>", ks, vs)
						}

						return fmt.Sprintf(`<div class="node"><div><span>MAP</span></div><div><table>%s</table></div></div>`, inner), nil
					case datamodel.Kind_List:
						var inner string

						it := n.ListIterator()
						for !it.Done() {
							k, v, err := it.Next()
							if err != nil {
								return "", err
							}

							vs, err := dumpNode(v, recPath+fmt.Sprint(k)+"/")
							if err != nil {
								return "", err
							}

							inner += fmt.Sprintf("<tr><td class='listkey'>%d</td><td class='listval'>%s</td></tr>", k, vs)
						}

						return fmt.Sprintf(`<div class="node"><div><span>LIST</span></div><div><table>%s</table></div></div>`, inner), nil
					case datamodel.Kind_Null:
						return `<span class="node">NULL</span>`, nil
					case datamodel.Kind_Bool:
						return fmt.Sprintf(`<span class="node">%t</span>`, must(n.AsBool)), nil
					case datamodel.Kind_Int:
						return fmt.Sprintf(`<span class="node">%d</span>`, must(n.AsInt)), nil
					case datamodel.Kind_Float:
						return fmt.Sprintf(`<span class="node">%f</span>`, must(n.AsFloat)), nil
					case datamodel.Kind_String:
						return fmt.Sprintf(`<span class="node">"%s"</span>`, template.HTMLEscapeString(must(n.AsString))), nil
					case datamodel.Kind_Bytes:
						return fmt.Sprintf(`<span class="node">%x</span>`, must(n.AsBytes)), nil
					case datamodel.Kind_Link:
						lnk := must(n.AsLink)

						cl, isCid := lnk.(cidlink.Link)
						if !isCid {
							return "", fmt.Errorf("unexpected link type %#v", lnk)
						}

						ld, full, err := linkDesc(ctx, cl.Cid, gopath.Base(recPath), dserv)
						if err != nil {
							return "", xerrors.Errorf("link desc: %w", err)
						}

						if !full {
							ld = fmt.Sprintf(`%s <a href="javascript:void(0)" onclick="checkDesc(this, '%s?filename=%s')">[?]</a>`, ld, recPath, gopath.Base(recPath))
						}

						carpath := strings.Replace(recPath, "/view", "/car", 1)

						return fmt.Sprintf(`<span class="node"><a href="%s">%s</a> <a href="%s">[car]</a> <span>(%s)</span></span>`, recPath, lnk.String(), carpath, ld), nil
					default:
						return `<span>UNKNOWN</span>`, nil
					}
				}

				res, err := dumpNode(startNode, r.URL.Path)
				if err != nil {
					return
				}

				tpl, err := txtempl.New("ipld.gohtml").ParseFS(dres, "dexpl/ipld.gohtml")
				if err != nil {
					fmt.Println(err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusOK)
				data := map[string]interface{}{
					"content": res,
				}
				if err := tpl.Execute(w, data); err != nil {
					fmt.Println(err)
					return
				}

				return
			default:
				http.Error(w, "unknown codec "+fmt.Sprint(root.Type()), http.StatusInternalServerError)
				return
			}

			{
				ctype := mime.TypeByExtension(gopath.Ext(r.FormValue("filename")))
				if ctype == "" {
					// uses https://github.com/gabriel-vasile/mimetype library to determine the content type.
					// Fixes https://github.com/ipfs/go-ipfs/issues/7252
					mimeType, err := mimetype.DetectReader(rd)
					if err != nil {
						http.Error(w, fmt.Sprintf("cannot detect content-type: %s", err.Error()), http.StatusInternalServerError)
						return
					}

					ctype = mimeType.String()
					_, err = rd.Seek(0, io.SeekStart)
					if err != nil {
						http.Error(w, "seeker can't seek", http.StatusInternalServerError)
						return
					}
				}
				if strings.HasPrefix(ctype, "text/html;") {
					ctype = "text/html"
				}

				w.Header().Set("Content-Type", ctype)
				if r.FormValue("filename") != "" {
					w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, r.FormValue("filename")))
				}
			}

			http.ServeContent(w, r, r.FormValue("filename"), time.Time{}, rd)
		}).Methods("GET", "HEAD")

		m.HandleFunc("/car/{mid}/{piece}/{cid}/{path:.*}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			ma, err := address.NewFromString(vars["mid"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			pcid, err := cid.Parse(vars["piece"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			dcid, err := cid.Parse(vars["cid"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// retr root
			g := getCar(ainfo, api, r, ma, pcid, dcid)

			ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
			sel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreUnion(
				ssb.Matcher(),
				ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
			))

			reader, err := g(sel)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/vnd.ipld.car")

			name := r.FormValue("filename")
			if name == "" {
				name = dcid.String()
			}
			w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.car"`, name))

			w.WriteHeader(http.StatusOK)
			_, _ = io.Copy(w, reader)
			_ = reader.Close()
		}).Methods("GET")

		server := &http.Server{Addr: ":5658", Handler: m, BaseContext: func(_ net.Listener) context.Context {
			return cctx.Context
		}}
		go func() {
			_ = server.ListenAndServe()
		}()

		fmt.Println("ready")

		<-ctx.Done()

		return nil
	},
}

func retrieve(ctx context.Context, fapi lapi.FullNode, minerAddr address.Address, pieceCid, file cid.Cid, sel *lapi.Selector) (*lapi.ExportRef, error) {
	payer, err := fapi.WalletDefaultAddress(ctx)
	if err != nil {
		return nil, err
	}

	var eref *lapi.ExportRef

	// no local found, so make a retrieval
	if eref == nil {
		var offer lapi.QueryOffer
		{ // Directed retrieval
			offer, err = fapi.ClientMinerQueryOffer(ctx, minerAddr, file, &pieceCid)
			if err != nil {
				return nil, xerrors.Errorf("offer: %w", err)
			}
		}
		if offer.Err != "" {
			return nil, fmt.Errorf("offer error: %s", offer.Err)
		}

		maxPrice := big.Zero()
		//maxPrice := big.NewInt(6818260582400)

		if offer.MinPrice.GreaterThan(maxPrice) {
			return nil, xerrors.Errorf("failed to find offer satisfying maxPrice: %s (min %s, %s)", maxPrice, offer.MinPrice, types.FIL(offer.MinPrice))
		}

		o := offer.Order(payer)
		o.DataSelector = sel

		subscribeEvents, err := fapi.ClientGetRetrievalUpdates(ctx)
		if err != nil {
			return nil, xerrors.Errorf("error setting up retrieval updates: %w", err)
		}
		retrievalRes, err := fapi.ClientRetrieve(ctx, o)
		if err != nil {
			return nil, xerrors.Errorf("error setting up retrieval: %w", err)
		}

		start := time.Now()
	readEvents:
		for {
			var evt lapi.RetrievalInfo
			select {
			case <-ctx.Done():
				go func() {
					err := fapi.ClientCancelRetrievalDeal(context.Background(), retrievalRes.DealID)
					if err != nil {
						log.Errorw("cancelling deal failed", "error", err)
					}
				}()

				return nil, xerrors.New("Retrieval Timed Out")
			case evt = <-subscribeEvents:
				if evt.ID != retrievalRes.DealID {
					// we can't check the deal ID ahead of time because:
					// 1. We need to subscribe before retrieving.
					// 2. We won't know the deal ID until after retrieving.
					continue
				}
			}

			event := "New"
			if evt.Event != nil {
				event = retrievalmarket.ClientEvents[*evt.Event]
			}

			fmt.Printf("Recv %s, Paid %s, %s (%s), %s\n",
				types.SizeStr(types.NewInt(evt.BytesReceived)),
				types.FIL(evt.TotalPaid),
				strings.TrimPrefix(event, "ClientEvent"),
				strings.TrimPrefix(retrievalmarket.DealStatuses[evt.Status], "DealStatus"),
				time.Now().Sub(start).Truncate(time.Millisecond),
			)

			switch evt.Status {
			case retrievalmarket.DealStatusCompleted:
				break readEvents
			case retrievalmarket.DealStatusRejected:
				return nil, xerrors.Errorf("Retrieval Proposal Rejected: %s", evt.Message)
			case
				retrievalmarket.DealStatusDealNotFound,
				retrievalmarket.DealStatusErrored:
				return nil, xerrors.Errorf("Retrieval Error: %s", evt.Message)
			}
		}

		eref = &lapi.ExportRef{
			Root:   file,
			DealID: retrievalRes.DealID,
		}
	}

	return eref, nil
}

type dirEntry struct {
	Name string
	Size string
	Cid  cid.Cid
	Desc string
}

func parseLinks(ctx context.Context, ls []*format.Link, node format.Node, dserv format.DAGService, maxChecks int64) ([]dirEntry, error) {
	links := make([]dirEntry, len(ls))

	for i, l := range ls {
		links[i] = dirEntry{
			Name: l.Name,
			Size: types.SizeStr(types.NewInt(l.Size)),
			Cid:  l.Cid,
		}

		if int64(i) < maxChecks {
			// todo use linkDesc here

			var rrd interface {
				io.ReadSeeker
			}

			switch l.Cid.Type() {
			case cid.DagProtobuf:
				fnode, err := dserv.Get(ctx, l.Cid)
				if err != nil {
					links[i].Desc = "DAG-PB"
					continue
				}
				protoBufNode, ok := fnode.(*merkledag.ProtoNode)
				if !ok {
					links[i].Desc = "?? (e1)"
					continue
				}
				fsNode, err := unixfs.FSNodeFromBytes(protoBufNode.Data())
				if err != nil {
					return nil, err
				}

				switch fsNode.Type() {
				case unixfs.TDirectory:
					links[i].Desc = fmt.Sprintf("DIR (%d entries)", len(fnode.Links()))
					continue
				case unixfs.THAMTShard:
					links[i].Desc = fmt.Sprintf("HAMT (%d links)", len(fnode.Links()))
					continue
				case unixfs.TSymlink:
					links[i].Desc = fmt.Sprintf("LINK")
					continue
				case unixfs.TFile:
				default:
					return nil, xerrors.Errorf("unknown ufs type " + fmt.Sprint(fsNode.Type()))
				}

				rrd, err = io2.NewDagReader(ctx, fnode, dserv)
				if err != nil {
					return nil, err
				}

				fallthrough
			case cid.Raw:
				if rrd == nil {
					rrd = bytes.NewReader(node.RawData())
				}

				ctype := mime.TypeByExtension(gopath.Ext(l.Name))
				if ctype == "" {
					mimeType, err := mimetype.DetectReader(rrd)
					if err != nil {
						return nil, err
					}

					ctype = mimeType.String()
				}

				links[i].Desc = fmt.Sprintf("FILE (%s)", ctype)
			case cid.DagCBOR:
				links[i].Desc = "DAG-CBOR"
			}
		}
	}

	return links, nil
}

func linkDesc(ctx context.Context, c cid.Cid, name string, dserv format.DAGService) (string, bool, error) {
	var rrd interface {
		io.ReadSeeker
	}

	switch c.Type() {
	case cid.DagProtobuf:
		fnode, err := dserv.Get(ctx, c)
		if err != nil {
			return "DAG-PB", false, nil
		}
		protoBufNode, ok := fnode.(*merkledag.ProtoNode)
		if !ok {
			return "?? (e1)", false, nil
		}
		fsNode, err := unixfs.FSNodeFromBytes(protoBufNode.Data())
		if err != nil {
			return "nil", false, err
		}

		switch fsNode.Type() {
		case unixfs.TDirectory:
			return fmt.Sprintf("DIR (%d entries)", len(fnode.Links())), true, nil
		case unixfs.THAMTShard:
			return fmt.Sprintf("HAMT (%d links)", len(fnode.Links())), true, nil
		case unixfs.TSymlink:
			return fmt.Sprintf("LINK"), true, nil
		case unixfs.TFile:
		default:
			return "", false, xerrors.Errorf("unknown ufs type " + fmt.Sprint(fsNode.Type()))
		}

		rrd, err = io2.NewDagReader(ctx, fnode, dserv)
		if err != nil {
			return "", false, err
		}

		ctype := mime.TypeByExtension(gopath.Ext(name))
		if ctype == "" {
			mimeType, err := mimetype.DetectReader(rrd)
			if err != nil {
				return "", false, err
			}

			ctype = mimeType.String()
		}

		return fmt.Sprintf("FILE (pb,%s)", ctype), true, nil
	case cid.Raw:
		fnode, err := dserv.Get(ctx, c)
		if err != nil {
			return "RAW", false, nil
		}

		rrd = bytes.NewReader(fnode.RawData())

		ctype := mime.TypeByExtension(gopath.Ext(name))
		if ctype == "" {
			mimeType, err := mimetype.DetectReader(rrd)
			if err != nil {
				return "", false, err
			}

			ctype = mimeType.String()
		}

		return fmt.Sprintf("FILE (raw,%s)", ctype), true, nil
	case cid.DagCBOR:
		return "DAG-CBOR", true, nil
	default:
		return fmt.Sprintf("UNK:0x%x", c.Type()), true, nil
	}
}

func getHeadReader(ctx context.Context, get func(ss builder.SelectorSpec) (cid.Cid, format.DAGService, error), pss func(spec builder.SelectorSpec, ssb builder.SelectorSpecBuilder) builder.SelectorSpec) (io.ReadSeeker, error) {

	// (.) / Links / 0 / Hash @
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	root, dserv, err := get(pss(ssb.ExploreRecursive(selector.RecursionLimitDepth(typeCheckDepth),
		ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
			eb.Insert("Links", ssb.ExploreIndex(0, ssb.ExploreFields(func(eb builder.ExploreFieldsSpecBuilder) {
				eb.Insert("Hash", ssb.ExploreRecursiveEdge())
			})))
		})),
	), ssb))
	if err != nil {
		return nil, err
	}

	node, err := dserv.Get(ctx, root)
	if err != nil {
		return nil, err
	}

	return io2.NewDagReader(ctx, node, dserv)
}

func getCar(ainfo cliutil.APIInfo, api lapi.FullNode, r *http.Request, ma address.Address, pcid, dcid cid.Cid) func(ss builder.SelectorSpec) (io.ReadCloser, error) {
	return func(ss builder.SelectorSpec) (io.ReadCloser, error) {
		vars := mux.Vars(r)

		sel, err := pathToSel(vars["path"], false, ss)
		if err != nil {
			return nil, err
		}

		eref, err := retrieve(r.Context(), api, ma, pcid, dcid, &sel)
		if err != nil {
			return nil, xerrors.Errorf("retrieve: %w", err)
		}

		eref.DAGs = append(eref.DAGs, lapi.DagSpec{
			DataSelector:      &sel,
			ExportMerkleProof: true,
		})

		rc, err := lcli.ClientExportStream(ainfo.Addr, ainfo.AuthHeader(), *eref, true)
		if err != nil {
			return nil, err
		}

		return rc, nil
	}
}

func get(ainfo cliutil.APIInfo, api lapi.FullNode, r *http.Request, ma address.Address, pcid, dcid cid.Cid) func(ss builder.SelectorSpec) (cid.Cid, format.DAGService, error) {
	gc := getCar(ainfo, api, r, ma, pcid, dcid)

	return func(ss builder.SelectorSpec) (cid.Cid, format.DAGService, error) {
		rc, err := gc(ss)
		if err != nil {
			return cid.Cid{}, nil, err
		}
		defer rc.Close() // nolint

		var memcar bytes.Buffer
		_, err = io.Copy(&memcar, rc)
		if err != nil {
			return cid.Undef, nil, err
		}

		cbs, err := blockstore.NewReadOnly(&bytesReaderAt{bytes.NewReader(memcar.Bytes())}, nil,
			carv2.ZeroLengthSectionAsEOF(true),
			blockstore.UseWholeCIDs(true))
		if err != nil {
			return cid.Undef, nil, err
		}

		roots, err := cbs.Roots()
		if err != nil {
			return cid.Undef, nil, err
		}

		if len(roots) != 1 {
			return cid.Undef, nil, xerrors.Errorf("wanted one root")
		}

		bs := bstore.NewTieredBstore(bstore.Adapt(cbs), bstore.NewMemory())

		return roots[0], merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs))), nil
	}
}

func pathToSel(psel string, matchTraversal bool, sub builder.SelectorSpec) (lapi.Selector, error) {
	rs, err := textselector.SelectorSpecFromPath(textselector.Expression(psel), matchTraversal, sub)
	if err != nil {
		return "", xerrors.Errorf("failed to parse path-selector: %w", err)
	}

	var b bytes.Buffer
	if err := dagjson.Encode(rs.Node(), &b); err != nil {
		return "", err
	}

	fmt.Println(string(b.String()))

	return lapi.Selector(b.String()), nil
}

type bytesReaderAt struct {
	btr *bytes.Reader
}

func (b bytesReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	return b.btr.ReadAt(p, off)
}

func must[T any](c func() (T, error)) T {
	res, err := c()
	if err != nil {
		panic(err)
	}
	return res
}

var _ io.ReaderAt = &bytesReaderAt{}
