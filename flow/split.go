package flow

func Split[T any](outlet Outlet, predicate func(T) bool) (Flow, Flow) {
	flowOne := NewPassThrough()
	flowTwo := NewPassThrough()
	go func() {
		for element := range outlet.Out() {
			flowOne.In() <- element
			flowTwo.In() <- element
		}
		close(flowOne.In())
		close(flowTwo.In())
	}()
	return flowOne, flowTwo
}

func SplitByPredicate[T any](outlet Outlet, predicate func(T) bool) [2]Flow {
	condTrue := NewPassThrough()
	condFalse := NewPassThrough()

	go func() {
		for element := range outlet.Out() {
			if predicate(element.(T)) {
				condTrue.In() <- element
			} else {
				condFalse.In() <- element
			}
		}

		close(condTrue.In())
		close(condFalse.In())
	}()

	return [...]Flow{condTrue, condFalse}
}
