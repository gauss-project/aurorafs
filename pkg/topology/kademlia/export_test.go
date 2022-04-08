package kademlia

var (
	TimeToRetry                 = &timeToRetry
	QuickSaturationPeers        = &quickSaturationPeers
	SaturationPeers             = &saturationPeers
	OverSaturationPeers         = &overSaturationPeers
	BootnodeOverSaturationPeers = &bootNodeOverSaturationPeers
	LowWaterMark                = &nnLowWatermark
	PruneOversaturatedBinsFunc  = func(k *Kad) func(uint8) {
		return k.pruneOversaturatedBins
	}
	GenerateCommonBinPrefixes = generateCommonBinPrefixes
	PeerPingPollTime          = &peerPingPollTime
)

type PeerFilterFunc = peerFilterFunc
