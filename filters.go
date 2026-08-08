package pubsubqueue

// Filter — предикат, применяемый перед публикацией. Возвращает true, если сообщение должно пройти.
type Filter func(m Message) bool

func zeroFilter(m Message) bool { return m.Value != 0 }
func evenFilter(m Message) bool { return m.Value%2 == 0 }
func oddFilter(m Message) bool  { return m.Value%2 != 0 }
