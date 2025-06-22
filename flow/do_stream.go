package flow

func DoStream(out Output, in Input) {
	go func() {
		for element := range out.Out() {
			in.In() <- element
		}
	}()
}
