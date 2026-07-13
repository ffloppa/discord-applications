package kz.ffloppa;

import discord4j.common.util.Snowflake;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.interaction.ButtonInteractionEvent;
import discord4j.core.event.domain.interaction.ModalSubmitInteractionEvent;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.object.component.ActionRow;
import discord4j.core.object.component.Button;
import discord4j.core.object.component.TextInput;
import discord4j.core.object.entity.Message;
import discord4j.core.object.entity.User;
import discord4j.core.object.entity.channel.GuildMessageChannel;
import discord4j.core.object.entity.channel.MessageChannel;
import discord4j.core.spec.EmbedCreateSpec;
import discord4j.core.spec.InteractionPresentModalSpec;
import discord4j.core.spec.MessageCreateSpec;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
    private static final Logger log = Loggers.getLogger(Main.class);
    public final static Snowflake sendRequestChannelId = Snowflake.of(1525827353538854972L);
    public final static Snowflake adminRequestsChannelId = Snowflake.of(1525828328739700836L);
    public final static Snowflake debuggingChannelId = Snowflake.of(1525910734016479503L);
    public final static Snowflake managingChannelId = Snowflake.of(1526101639239438418L);

    public final static Snowflake guildId = Snowflake.of(1525736606315708476L);

    public final static Snowflake adminRoleId = Snowflake.of(1525737204163543191L);
    public final static Snowflake verifiedRoleId = Snowflake.of(1525825482556309585L);
    public final static Button sendRequestButton = Button.primary("send-request", "Отправить запрос");

    static void main() {
        GatewayDiscordClient client = DiscordClient.create("d")
                .gateway()
                .withEventDispatcher(d -> d.on(ReadyEvent.class)
                        .doOnNext(readyEvent -> log.info("Ready: {}", readyEvent.getShardInfo())))
                .login()
                .doOnError(e -> log.error("Failed to authenticate with Discord", e))
                .doOnSuccess(gateway -> gateway.getChannelById(debuggingChannelId).ofType(MessageChannel.class).subscribe(channel -> channel.createMessage("здарова дон")))
                .block();

        assert client != null;

        FamilyUsersChecker usersChecker = new FamilyUsersChecker(client);

        client.getChannelById(sendRequestChannelId)
                .ofType(MessageChannel.class)
                .flatMap(messageChannel ->
                        messageChannel.getMessagesBefore(Snowflake.of(Instant.now()))
                                .take(1)
                                .next()
                                .map(Message::getEmbeds)
                                .map(embeds -> !embeds.isEmpty() && embeds.get(0).getTitle().orElse("").contains("Los Pollos Hermanos"))
                                .defaultIfEmpty(false)
                                .flatMap(hasWelcomeMessage -> {
                                    if (!hasWelcomeMessage) {
                                        return messageChannel.createMessage(MessageCreateSpec.builder()
                                                .addEmbed(EmbedCreateSpec.builder()
                                                        .title("Welcome to Los Pollos Hermanos Family")
                                                        .description("Чтобы увидеть все каналы, подай заявку, жми 👇")
                                                        .build())
                                                .addComponent(ActionRow.of(sendRequestButton))
                                                .build());
                                    }
                                    return Mono.empty();
                                })
                )
                .subscribe(
                        _ -> log.info("Welcome message has been sent"),
                        err -> client.getChannelById(debuggingChannelId)
                                .ofType(GuildMessageChannel.class)
                                .flatMap(channel -> channel.createMessage(EmbedCreateSpec.builder()
                                        .title("ашибка дон")
                                        .description(err.getMessage())
                                        .build())
                                )
                );

        client.on(ModalSubmitInteractionEvent.class, event -> {
            if (event.getCustomId().equals("fill-request")) {

                String nick = "???";
                for (TextInput input : event.getComponents(TextInput.class)) {
                    if (input.getCustomId().equals("nickname")) {
                        nick = input.getValue().orElse("???");
                    }
                }

                String stringifiedUserId = event.getUser().getId().asString();
                Button approveButton = Button.success(String.format("approve-request-%s-%s", stringifiedUserId, nick), "✅");
                Button denyButton = Button.danger(String.format("deny-request-%s", stringifiedUserId), "❌");

                final String finalNick = nick;
                Mono<Message> sendToAdmins = client.getChannelById(adminRequestsChannelId)
                        .ofType(GuildMessageChannel.class)
                        .flatMap(channel -> channel.createMessage(MessageCreateSpec.builder()
                                .addEmbed(EmbedCreateSpec.builder()
                                        .title("Заявка")
                                        .description(String.format("Пользователь: <@%s>\nНик: **%s**", stringifiedUserId, finalNick))
                                        .thumbnail(event.getUser().getAvatarUrl())
                                        .build())
                                .addComponent(ActionRow.of(approveButton, denyButton))
                                .build()));

                return event.reply("Заявка отправлена, рассмотрим за сутки")
                        .withEphemeral(true)
                        .then(sendToAdmins);
            }
            return Mono.empty();
        }).subscribe();

        TextInput nickInput = TextInput.small("nickname", "Кто вы?")
                .placeholder("Знакомьтесь с клавиатурой Gboard! Здесь будет сохраняться текст, который вы копируете.");

        client.on(ButtonInteractionEvent.class, event -> {
            String eventId = event.getCustomId();

            if (eventId.equals("send-request")) {
                return event.presentModal(InteractionPresentModalSpec.builder()
                        .title("Подача заявки на подлинность")
                        .customId("fill-request")
                        .addComponent(ActionRow.of(nickInput))
                        .build()
                );
            }

            if (eventId.startsWith("approve-request-")) {
                AtomicBoolean isAdmin = new AtomicBoolean();
                event.getUser().asMember(guildId)
                      .flatMap(member -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
                      .subscribe(isAdmin::set);
                if(isAdmin.get()) {
                    String[] data = eventId.replace("approve-request-", "").split("-");

                    String userId = data[0];
                    String nickName = data[1];

                    return client.getGuildById(guildId)
                            .flatMap(guild -> guild.getMemberById(Snowflake.of(userId)))
                            .flatMap(member -> member.edit().name(nickName))
                            .flatMap(member -> member.addRole(verifiedRoleId))
                            .flatMap(_ -> usersChecker.createDBRecordUser(client,userId,nickName))
                            .then(event.deferEdit())
                            .then(Mono.justOrEmpty(event.getInteraction().getMessage())
                                    .flatMap(msg -> msg.edit().withComponents(Collections.emptyList()))
                            )
                            .then()
                            .onErrorResume(err -> client.getChannelById(debuggingChannelId)
                                    .ofType(GuildMessageChannel.class)
                                    .flatMap(channel -> channel.createMessage(EmbedCreateSpec.builder()
                                            .title("ашибка дон")
                                            .description(err.getMessage() + "\n\n" + userId)
                                            .build())
                                    ).then()
                            );
                } else {
                    return event.reply("No access").withEphemeral(true);
                }
            }

            if (eventId.startsWith("deny-request-")) {
                AtomicBoolean isAdmin = new AtomicBoolean();
                event.getUser().asMember(guildId)
                        .flatMap(member -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
                        .subscribe(isAdmin::set);
                if(isAdmin.get()) {
                    String userId = eventId.replace("deny-request-", "");

                    return client.getUserById(Snowflake.of(userId))
                            .flatMap(User::getPrivateChannel)
                            .flatMap(channel -> channel.createMessage("заявька атклонена дон"))
                            .then(event.deferEdit())
                            .then(Mono.justOrEmpty(event.getInteraction().getMessage())
                                    .flatMap(msg -> msg.edit().withComponents(Collections.emptyList()))
                            )
                            .then()
                            .onErrorResume(err -> client.getChannelById(debuggingChannelId)
                                    .ofType(GuildMessageChannel.class)
                                    .flatMap(channel -> channel.createMessage(EmbedCreateSpec.builder()
                                            .title("ашибка дон")
                                            .description(err.getMessage() + "\n\n" + userId)
                                            .build())
                                    ).then()
                            );
                } else {
                    return event.reply("No access").withEphemeral(true);
                }
            }

            return Mono.empty();
        }).subscribe();

        client.onDisconnect().subscribe(_ -> client.getChannelById(debuggingChannelId)
                .ofType(GuildMessageChannel.class)
                .subscribe(channel -> channel.createMessage("все дон я пошел"))
        );
    }
}

