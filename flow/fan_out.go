package flow

func FanOut(outlet Outlet, magnitude int) []Flow {
	out := make([]Flow, magnitude)
	for i := 0; i < magnitude; i++ {
		out[i] = NewPassThrough()
	}

	go func() {
		for element := range outlet.Out() {
			for _, flow := range out {
				flow.In() <- element
			}
		}
		for _, flow := range out {
			close(flow.In())
		}
	}()

	return out
}
