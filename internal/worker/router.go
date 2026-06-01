package worker

// Router routes a notification type to one or more channels.
type Router struct {
	routes map[string][]string
}

func NewRouter() *Router {
	return &Router{
		routes: map[string][]string{
			NotificationTypeVerifyEmail:   {ChannelEmail},
			NotificationTypeResetPassword: {ChannelEmail},
			NotificationTypeSetPassword:   {ChannelEmail},
		},
	}
}

func (r *Router) DetermineChannels(notificationType string) []string {
	if channels, ok := r.routes[notificationType]; ok {
		return channels
	}
	return []string{}
}
