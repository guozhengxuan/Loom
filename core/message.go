package core

import (
	"Wahoo++/crypto"
	"Wahoo++/pool"
	"bytes"
	"encoding/gob"
	"reflect"
	"strconv"
)

type Message interface {
	MsgType() int
}

// Definition for network messages sent from node to node.
type NetMessage interface {
	Message
	Hash() crypto.Digest
}

type Slot struct {
	Author NodeID
	Height int
}

type Header struct {
	Slot      Slot
	Round     int // Block round
	FirstRefH int // Height of the first block of round R
}

type Block struct {
	Header Header
	Batch  pool.Batch
	Ref    []Header
	Sig    crypto.Signature
}

func NewBlock(
	author NodeID,
	height int,
	round int,
	firstH int,
	batch pool.Batch,
	ref []Header,
	sigService *crypto.SigService,
) (*Block, error) {

	slot := Slot{author, height}

	block := &Block{
		Header: Header{slot, round, firstH},
		Batch:  batch,
		Ref:    ref,
	}

	if sig, err := sigService.RequestSignature(block.Hash()); err != nil {
		return nil, err
	} else {
		block.Sig = sig
		return block, nil
	}
}

func (b *Block) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(buf).Encode(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *Block) Decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(b); err != nil {
		return err
	}
	return nil
}

func (b *Block) Verify(committee Committee) bool {
	return b.Sig.Verify(committee.Name(b.Header.Slot.Author), b.Hash())
}

func (b *Block) Hash() crypto.Digest {

	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(b.Header.Slot.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(b.Header.Slot.Height), 2))
	for _, tx := range b.Batch.Txs {
		hasher.Add(tx)
	}
	// for d, id := range b.Reference {
	// 	hasher.Add(d[:])
	// 	hasher.Add(strconv.AppendInt(nil, int64(id), 2))
	// }

	return hasher.Sum256(nil)
}

func (b *Block) MsgType() int {
	return ProposeType
}

// Echo
type Echo struct {
	Author NodeID
	Header Header
	Sig    crypto.Signature
}

func NewEcho(
	author NodeID,
	block *Block,
	sigService *crypto.SigService,
) (*Echo, error) {
	e := &Echo{
		Author: author,
		Header: block.Header,
	}
	sig, err := sigService.RequestSignature(e.Hash())
	if err != nil {
		return nil, err
	}
	e.Sig = sig
	return e, nil
}

func (msg *Echo) Verify(committee Committee) bool {
	return msg.Sig.Verify(committee.Name(msg.Author), msg.Hash())
}

func (msg *Echo) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(msg.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(msg.Header.Slot.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(msg.Header.Slot.Height), 2))
	return hasher.Sum256(nil)
}

func (msg *Echo) MsgType() int {
	return EchoType
}

// Elect
type Elect struct {
	Author   NodeID
	Round    int
	SigShare crypto.SignatureShare
}

func NewElectMsg(Author NodeID, Round int, sigService *crypto.SigService) (*Elect, error) {
	e := &Elect{
		Author: Author,
		Round:  Round,
	}
	share, err := sigService.RequestTsSugnature(e.Hash())
	if err != nil {
		return nil, err
	}
	e.SigShare = share

	return e, nil
}

func (e *Elect) Verify() bool {
	return e.SigShare.Verify(e.Hash())
}

func (e *Elect) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(e.Round), 2))
	return hasher.Sum256(nil)
}

func (msg *Elect) MsgType() int {
	return ElectType
}

// RequestBlock
type RequestBlockMsg struct {
	Author    NodeID
	MissBlock []crypto.Digest
	Signature crypto.Signature
	ReqID     int
	Ts        int64
}

func NewRequestBlock(
	Author NodeID,
	MissBlock []crypto.Digest,
	ReqID int,
	Ts int64,
	sigService *crypto.SigService,
) (*RequestBlockMsg, error) {
	msg := &RequestBlockMsg{
		Author:    Author,
		MissBlock: MissBlock,
		ReqID:     ReqID,
		Ts:        Ts,
	}
	sig, err := sigService.RequestSignature(msg.Hash())
	if err != nil {
		return nil, err
	}
	msg.Signature = sig
	return msg, nil
}

func (msg *RequestBlockMsg) Verify(committee Committee) bool {
	return msg.Signature.Verify(committee.Name(msg.Author), msg.Hash())
}

func (msg *RequestBlockMsg) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(msg.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(msg.ReqID), 2))
	for _, d := range msg.MissBlock {
		hasher.Add(d[:])
	}
	return hasher.Sum256(nil)
}

func (msg *RequestBlockMsg) MsgType() int {
	return RequestBlockType
}

// ReplyBlockMsg
type ReplyBlockMsg struct {
	Author    NodeID
	Blocks    []*Block
	ReqID     int
	Signature crypto.Signature
}

func NewReplyBlockMsg(Author NodeID, B []*Block, ReqID int, sigService *crypto.SigService) (*ReplyBlockMsg, error) {
	msg := &ReplyBlockMsg{
		Author: Author,
		Blocks: B,
		ReqID:  ReqID,
	}
	sig, err := sigService.RequestSignature(msg.Hash())
	if err != nil {
		return nil, err
	}
	msg.Signature = sig
	return msg, nil
}

func (msg *ReplyBlockMsg) Verify(committee Committee) bool {
	return msg.Signature.Verify(committee.Name(msg.Author), msg.Hash())
}

func (msg *ReplyBlockMsg) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(msg.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(msg.ReqID), 2))
	return hasher.Sum256(nil)
}

func (msg *ReplyBlockMsg) MsgType() int {
	return ReplyBlockType
}

type LoopBackMsg struct {
	BlockHash crypto.Digest
}

func (msg *LoopBackMsg) Hash() crypto.Digest {
	return crypto.NewHasher().Sum256(msg.BlockHash[:])
}

func (msg *LoopBackMsg) MsgType() int {
	return LoopBackType
}

// Definition for inner messages passed between core, dag and commitor.
type checkReq struct {
	items      []Header
	missRespCh chan<- []Header
}

func (r *checkReq) MsgType() int {
	return CheckReqType
}

type refReq struct {
	round     int
	refRespCh chan<- []Header
}

func (r *refReq) MsgType() int {
	return RefReqType
}

type commitReq struct {
	leader NodeID
	round  int
}

func (r *commitReq) MsgType() int {
	return CommitReqType
}

type blockPullReq struct {
	slot        Slot
	blockPullCh chan<- *Block
}

func (r *blockPullReq) MsgType() int {
	return BlockReqType
}

type leaderReq struct {
	round        int
	leaderPullCh chan<- NodeID
}

func (r *leaderReq) MsgType() int {
	return LeaderReqType
}

type gcReq struct {
	round        int
	newWatermark map[NodeID]int
}

func (r *gcReq) MsgType() int {
	return CleanReqType
}

// Define type enum for all messages
const (
	// Network messages
	ProposeType int = iota
	EchoType
	ElectType
	RequestBlockType
	ReplyBlockType
	LoopBackType

	// Dag & commiter messages
	CheckReqType
	RefReqType
	CommitReqType

	BlockReqType
	LeaderReqType
	CleanReqType

	TotalNums
)

var DefaultNetMsgTypes = map[int]reflect.Type{
	ProposeType:      reflect.TypeOf(Block{}),
	EchoType:         reflect.TypeOf(Echo{}),
	ElectType:        reflect.TypeOf(Elect{}),
	RequestBlockType: reflect.TypeOf(RequestBlockMsg{}),
	ReplyBlockType:   reflect.TypeOf(ReplyBlockMsg{}),
	LoopBackType:     reflect.TypeOf(LoopBackMsg{}),
}
