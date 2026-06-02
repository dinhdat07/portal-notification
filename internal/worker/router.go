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

func (r *Router) DetermineChannels(event NotificationRequestedEvent) []string {
	// Routing for Announcements (dynamic policy resolver based on event data)
	if event.NotificationType == NotificationTypeAnnouncement {
		annType, _ := event.Data["type"].(string)
		switch annType {
		case "INFO", "WARNING":
			return []string{ChannelEmail}
		case "ALERT":
			return []string{ChannelEmail, ChannelTelegram}
		default:
			return []string{ChannelEmail} // Fallback
		}
	}

	if channels, ok := r.routes[event.NotificationType]; ok {
		return channels
	}
	return []string{}
}
