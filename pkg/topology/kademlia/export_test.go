package kademlia

var (
	TimeToRetry                 = &timeToRetry
	QuickSaturationPeers        = &quickSaturationPeers
	SaturationPeers             = &saturationPeers
	OverSaturationPeers         = &overSaturationPeers
	BootnodeOverSaturationPeers = &bootNodeOverSaturationPeers
	PruneOversaturatedBinsFunc  = func(k *Kad) func(uint8) {
		return k.pruneOversaturatedBins
	}
	GenerateCommonBinPrefixes = generateCommonBinPrefixes
)
