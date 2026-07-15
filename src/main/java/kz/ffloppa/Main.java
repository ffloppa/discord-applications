package kz.ffloppa;

import discord4j.common.util.Snowflake;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.interaction.ButtonInteractionEvent;
import discord4j.core.event.domain.interaction.ChatInputInteractionEvent;
import discord4j.core.event.domain.interaction.ModalSubmitInteractionEvent;
import discord4j.core.event.domain.lifecycle.DisconnectEvent;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.object.command.ApplicationCommandOption;
import discord4j.core.object.component.ActionRow;
import discord4j.core.object.component.Button;
import discord4j.core.object.component.TextDisplay;
import discord4j.core.object.component.TextInput;
import discord4j.core.object.entity.Message;
import discord4j.core.object.entity.User;
import discord4j.core.object.entity.channel.GuildMessageChannel;
import discord4j.core.object.entity.channel.MessageChannel;
import discord4j.core.spec.EmbedCreateSpec;
import discord4j.core.spec.InteractionPresentModalSpec;
import discord4j.core.spec.MessageCreateSpec;
import discord4j.discordjson.json.ApplicationCommandOptionData;
import discord4j.discordjson.json.ApplicationCommandRequest;
import discord4j.gateway.intent.Intent;
import discord4j.gateway.intent.IntentSet;
import io.github.cdimascio.dotenv.Dotenv;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
    public static final Logger log = Loggers.getLogger(Main.class);
    public static final Dotenv dotenv = Dotenv.load();
    public final static Snowflake sendRequestChannelId = Snowflake.of(1525827353538854972L);
    public final static Snowflake adminRequestsChannelId = Snowflake.of(1525828328739700836L);
    public final static Snowflake debuggingChannelId = Snowflake.of(1525910734016479503L);
    public final static Snowflake globalMaintenanceChannelId = Snowflake.of(1526300681479389184L);
    public final static Snowflake managingChannelId = Snowflake.of(1526101639239438418L);

    public final static Snowflake guildId = Snowflake.of(1525736606315708476L);

    public final static Snowflake adminRoleId = Snowflake.of(1525737204163543191L);
    public final static Snowflake verifiedRoleId = Snowflake.of(1525825482556309585L);
    public final static Button sendRequestButton = Button.primary("send-request", "заделать заявительный запрос");
    public final static Button prikolButton = Button.primary("prikol", "Я гражданин Чеченской Республики");

    public static void main(String[] args) {
        GatewayDiscordClient client = DiscordClient.create(dotenv.get("token"))
                .gateway()
                .setEnabledIntents(IntentSet.of(
                        Intent.GUILDS,
                        Intent.GUILD_MEMBERS,
                        Intent.GUILD_MESSAGES,
                        Intent.MESSAGE_CONTENT
                ))
                .withEventDispatcher(d -> d.on(ReadyEvent.class)
                        .flatMap(readyEvent -> {
                            log.info("Ready: {}", readyEvent.getShardInfo());

                            long applicationId = readyEvent.getClient().getRestClient().getApplicationId().block();

                            var whoCommand = ApplicationCommandRequest.builder()
                                    .name("who")
                                    .description("понимать ба?")
                                    .addOption(ApplicationCommandOptionData.builder().description("понимать ба?").name("user").type(ApplicationCommandOption.Type.USER.getValue()).build())
                                    .build();

                            readyEvent.getClient().getRestClient().getApplicationService().createGuildApplicationCommand(applicationId, guildId.asLong(), whoCommand).subscribe();

                            var editCommand = ApplicationCommandRequest.builder()
                                    .name("edit_nicknames")
                                    .description("понимать ба?")
                                    .build();

                            readyEvent.getClient().getRestClient().getApplicationService().createGuildApplicationCommand(applicationId, guildId.asLong(), editCommand).subscribe();

                            return readyEvent.getClient()
                                    .getChannelById(globalMaintenanceChannelId)
                                    .ofType(GuildMessageChannel.class)
                                    .flatMap(channel -> channel.createMessage("привет ба"));
                        }))
                .login()
                .doOnError(e -> log.error("Failed to authenticate with Discord", e))
                .doOnSuccess(gateway -> gateway.getChannelById(debuggingChannelId).ofType(MessageChannel.class).flatMap(channel -> channel.createMessage(LocalTime.now() + ", здарова дон")))
                .block();

        assert client != null;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                client.getChannelById(globalMaintenanceChannelId)
                        .ofType(GuildMessageChannel.class)
                        .flatMap(channel -> channel.createMessage("мнъе поьрэ засунуть ба"))
                        .block();
            } catch (Exception e) {
                log.error("Не удалось отправить сообщение перед выходом", e);
            }
        }));
        FamilyUsersChecker usersChecker = new FamilyUsersChecker(client);

        client.on(ChatInputInteractionEvent.class, event -> {
            if (event.getCommandName().equals("who")) {
                Mono<User> targetUserMono = event.getOptionAsUser("user");
                return event.getUser().asMember(guildId)
                        .flatMap(member -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
                        .flatMap(isAdmin -> {
                            if (isAdmin)
                                return targetUserMono.flatMap(user -> {
                                    String targetUserId = user.getId().asString();

                                    return event.reply(String.format("<@%s> - **%s**", targetUserId, usersChecker.getUserNameFromDB(targetUserId))).withEphemeral(true);
                                });
                            else
                                return event.reply("No permission").withEphemeral(true);
                        });
            }
            if (event.getCommandName().equals("edit_nicknames")) {
                return event.getUser().asMember(guildId)
                        .flatMap(member -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
                        .flatMap(isAdmin -> {
                            if (isAdmin)
                                return event.deferReply()
                                        .withEphemeral(true)
                                        .then(
                                                usersChecker.callEditUsersMenu(client, event)
                                        );
                            else
                                return event.reply("No permission").withEphemeral(true);
                        });
            }

            return Mono.empty();
        }).subscribe();

        client.getChannelById(sendRequestChannelId)
                .ofType(MessageChannel.class)
                .flatMap(messageChannel ->
                        messageChannel.getMessagesBefore(Snowflake.of(Instant.now()))
                                .take(1)
                                .next()
                                .map(Message::getEmbeds)
                                .map(embeds -> !embeds.isEmpty() && embeds.get(1).getTitle().orElse("").contains("слышишь мультифрукт произведи махiнацию"))
                                .defaultIfEmpty(false)
                                .flatMap(hasWelcomeMessage -> {
                                    if (!hasWelcomeMessage) {
                                        return messageChannel.createMessage(MessageCreateSpec.builder()
                                                .addEmbed(EmbedCreateSpec.builder()
                                                        .title("Welcome to Los Pollos Hermanos Family")
                                                        .description("слышишь мультифрукт произведи махiнацию 👇")
                                                        .build())
                                                .addComponent(ActionRow.of(sendRequestButton,prikolButton))
                                                .build());
                                    }
                                    return Mono.empty();
                                })
                )
                .subscribe(
                        a -> log.info("Welcome message has been sent"),
                        err -> client.getChannelById(debuggingChannelId)
                                .ofType(GuildMessageChannel.class)
                                .flatMap(channel -> channel.createMessage(EmbedCreateSpec.builder()
                                        .title("ашибка дон")
                                        .description(err.getMessage())
                                        .build())
                                )
                );

        client.on(ModalSubmitInteractionEvent.class, event -> {
            if (event.getCustomId().equals("prikol-2")) {
                return event.reply("ты ба, в зәпъэчкался")
                        .withEphemeral(true)
                        .flatMap(mono ->
                            event.getUser().asMember(guildId)
                                    .flatMap(member -> member.addRole(Snowflake.of(1526537777670389850L)))
                                    .then(
                                            client.getChannelById(debuggingChannelId)
                                                    .ofType(GuildMessageChannel.class)
                                                    .flatMap(channel -> channel.createMessage(
                                                            String.format("нъовый грәждьанин <@%s>", event.getUser().getId().asString())
                                                    ))
                                    )
                        );
            }
            if (event.getCustomId().equals("fill-request")) {

                String nick = "???";
                for (TextInput input : event.getComponents(TextInput.class)) {
                    if (input.getCustomId().equals("nickname")) {
                        nick = input.getValue().orElse("???");
                    }
                }

                String stringifiedUserId = event.getUser().getId().asString();
                Button approveButton = Button.success(String.format("approve-request-%s-%s", stringifiedUserId, nick), "✅");
                Button rapproveButton = Button.success(String.format("rapprove-request-%s-%s", stringifiedUserId, nick), "✅ и провэстi мозгъовой әктивность");
                Button denyButton = Button.danger(String.format("deny-request-%s", stringifiedUserId), "❌");

                final String finalNick = nick;
                Mono<Message> sendToAdmins = client.getChannelById(adminRequestsChannelId)
                        .ofType(GuildMessageChannel.class)
                        .flatMap(channel -> channel.createMessage(MessageCreateSpec.builder()
                                .addEmbed(EmbedCreateSpec.builder()
                                        .title("заявительный запрос")
                                        .description(String.format("мультифрукт: <@%s>\nраспрострәнятся: **%s**", stringifiedUserId, finalNick))
                                        .thumbnail(event.getUser().getAvatarUrl())
                                        .build())
                                .addComponent(ActionRow.of(approveButton, rapproveButton, denyButton))
                                .build()));

                return event.reply("мультифрукт жду, согләсовәния зәявительного зәпроса")
                        .withEphemeral(true)
                        .then(sendToAdmins);
            }
            return Mono.empty();
        }).subscribe();

        TextInput nickInput = TextInput.small("nickname", "как называтькаться ба?")
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

            if(eventId.equals("prikol")) {
                return event.getUser().asMember(guildId)
                        .flatMap(member -> member.getRoles().any(role -> role.getId().equals(Snowflake.of(1526537777670389850L))))
                        .flatMap(isAlreadyHave -> {
                            if(!isAlreadyHave)
                                return event.presentModal(InteractionPresentModalSpec.builder()
                                        .title(" ")
                                        .customId("prikol-2")
                                        .addComponent(TextDisplay.of("Мои поздравльяния, ты проьпустил зәявьительный пункът, чтобә прьойти вопросәительный знъак нәпиши \"бурмалда\" "))
                                        .build());
                            else
                                return event.reply("ты нә приколье ба?").withEphemeral(true);
                            }
                        );
            }

            if (eventId.startsWith("rapprove-request-")) {
                return event.getUser().asMember(guildId)
                      .flatMap(member -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
                      .flatMap(isAdmin -> {
                          if(isAdmin) {
                              String[] data = eventId.replace("rapprove-request-", "").split("-");
                              String userId = data[0];
                              String nickName = data[1];

                              return event.deferEdit()
                                      .then(
                                              client.getGuildById(guildId)
                                                      .flatMap(guild -> guild.getMemberById(Snowflake.of(userId)))
                                                      .flatMap(member -> member.edit().withNicknameOrNull(nickName))
                                                      .flatMap(member -> member.addRole(verifiedRoleId))
                                                      .then(Mono.defer(() -> usersChecker.createDBRecordUser(client, userId, nickName)))
                                      )
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
                      });
            }

            if (eventId.startsWith("approve-request-")) {
                return event.getUser().asMember(guildId)
                        .flatMap(member -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
                        .flatMap(isAdmin -> {
                            if(isAdmin) {
                                String[] data = eventId.replace("approve-request-", "").split("-");

                                String userId = data[0];
                                String nickName = data[1];

                                return event.deferEdit()
                                        .then(
                                                client.getGuildById(guildId)
                                                        .flatMap(guild -> guild.getMemberById(Snowflake.of(userId)))
                                                        .flatMap(member -> member.edit().withNicknameOrNull(nickName))
                                                        .flatMap(member -> member.addRole(verifiedRoleId))
                                                        .then(Mono.defer(() -> usersChecker.createDBRecordUser(client, userId, nickName)))
                                        )
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
                        });
            }

            if (eventId.startsWith("deny-request-")) {
                return event.getUser().asMember(guildId)
                        .flatMap(member -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
                        .flatMap(isAdmin -> {
                            if (isAdmin) {
                                String userId = eventId.replace("deny-request-", "");

                                return client.getUserById(Snowflake.of(userId))
                                        .flatMap(User::getPrivateChannel)
                                        .flatMap(channel -> channel.createMessage("замешкалась админ и сказал не подходи"))
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
                        });
            }

            return Mono.empty();
        }).subscribe();

        client.onDisconnect().block();
    }
}

