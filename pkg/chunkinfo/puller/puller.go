package puller

//PullMax 最大拉取数 pulling+pulled
const PullMax = 200

//PullingMax 最大并行拉取数
const PullingMax = 10

//PullerMax 为队列拉取最大数
const PullerMax = 1000

type Puller struct {
	RootCid string
	Puller  []string
	UnPull  []*string
	Pulling []*string
	Pulled  []*string
}

func New(rootCid string) *Puller {
	return &Puller{
		RootCid: rootCid,
		Puller:  make([]string, 0, PullerMax),
		UnPull:  make([]*string, 0, PullerMax),
		Pulling: make([]*string, 0, PullingMax),
		Pulled:  make([]*string, 0, PullMax),
	}
}
