package mock

type Auth struct {
	AuthorizeFunc   func(string) bool
	GenerateKeyFunc func(string) (string, error)
}

func (ma *Auth) Authorize(u string) bool {
	if ma.AuthorizeFunc == nil {
		return true
	}
	return ma.AuthorizeFunc(u)
}
func (ma *Auth) GenerateKey(k string, _ int) (string, error) {
	if ma.GenerateKeyFunc == nil {
		return "", nil
	}
	return ma.GenerateKeyFunc(k)
}
func (ma *Auth) RefreshKey(k string, _ int) (string, error) {
	if ma.GenerateKeyFunc == nil {
		return "", nil
	}
	return ma.GenerateKeyFunc(k)
}
func (*Auth) Enforce(string, string, string) (bool, error) {
	return false, nil
}
