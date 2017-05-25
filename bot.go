package main

import (
	"github.com/nlopes/slack"
)

type bot struct {
	sender  *slack.RTM
	channel string
}

func initBot(token string) *bot {
	api := slack.New(token)

	bot := new(bot)

	bot.sender = api.NewRTM()

	go bot.sender.ManageConnection()

	return bot
}

func (bot *bot) receive() (ch chan string) {
	ch = make(chan string)

	go func() {
		for msg := range bot.sender.IncomingEvents {
			switch ev := msg.Data.(type) {
			case *slack.MessageEvent:
				if ev.Msg.SubType == `message_deleted` {
					continue
				}
				bot.channel = ev.Msg.Channel
				ch <- ev.Msg.Text
			default:
			}
		}
	}()

	return
}

func (bot *bot) send(msg string) {
	bot.sender.SendMessage(bot.sender.NewOutgoingMessage(msg, bot.channel))
}
