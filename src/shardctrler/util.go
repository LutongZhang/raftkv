package shardctrler

func getConfig(cfgs []Config,num int)*Config{
	if num == -1 || num >= len(cfgs){
		return &cfgs[len(cfgs)-1]
	} else{
		return &cfgs[num]
	}
}