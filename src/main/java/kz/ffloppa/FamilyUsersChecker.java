package kz.ffloppa;

import discord4j.common.util.Snowflake;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.guild.BanEvent;
import discord4j.core.event.domain.interaction.ChatInputInteractionEvent;
import discord4j.core.event.domain.interaction.ModalSubmitInteractionEvent;
import discord4j.core.event.domain.interaction.SelectMenuInteractionEvent;
import discord4j.core.object.component.ActionRow;
import discord4j.core.object.component.SelectMenu;
import discord4j.core.object.component.TextInput;
import discord4j.core.object.entity.Guild;
import discord4j.core.object.entity.channel.GuildMessageChannel;
import discord4j.core.object.entity.channel.MessageChannel;
import discord4j.core.spec.EmbedCreateSpec;
import discord4j.core.spec.InteractionPresentModalSpec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.SQLException;
import java.util.*;

import static kz.ffloppa.Main.*;

public class FamilyUsersChecker {
    private final Database db = new Database();
    private final HashMap<String, String> users = new HashMap<>();
    private final Collection<String> usersIdxToCheck = new ArrayList<>();

    FamilyUsersChecker(GatewayDiscordClient client) {

        client.on(SelectMenuInteractionEvent.class, event -> {
            if (event.getCustomId().equals("menu-edit-nicks-users")) {
                List<String> selectedValues = event.getValues();
                String choice = selectedValues.getFirst();

                if (isUserExists(client, choice)) return event.reply("этого я помню").withEphemeral(true);

                TextInput nickInput = TextInput.small("nickname", "Кьо этость?")
                        .placeholder("Введите новый никнейм");

                return event.presentModal(InteractionPresentModalSpec.builder()
                        .title("выставь ник дон")
                        .customId("fill-user-nick-" + choice)
                        .addComponent(ActionRow.of(nickInput))
                        .build()
                );
            }
            return Mono.empty();
        }).subscribe();

        client.on(ModalSubmitInteractionEvent.class, event -> {
            if (event.getCustomId().startsWith("fill-user-nick-")) {
                client.getGuildById(guildId)
                        .flatMap(guild -> guild.getMemberById(Snowflake.of(event.getCustomId().replace("fill-user-nick-", "")))
                                .flatMap(member -> {

                                    if (!member.isBot())
                                        return event.getUser().asMember(guildId)
                                                .flatMap(member1 -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
                                                .flatMap(isAdmin -> {
                                                    if (isAdmin) {
                                                        String user = event.getCustomId().replace("fill-user-nick-", "");
                                                        String nick = "???";

                                                        for (TextInput input : event.getComponents(TextInput.class)) {
                                                            if (input.getCustomId().equals("nickname")) {
                                                                nick = input.getValue().orElse("???");
                                                            }
                                                        }

                                                        String finalUser = user;
                                                        String finalNick = nick;
                                                        return client.getGuildById(guildId)
                                                                .flatMap(guild1 -> guild1.getMemberById(Snowflake.of(finalUser)))
                                                                .flatMap(member1 -> member1.edit().withNicknameOrNull(finalNick))
                                                                .then(Mono.defer(() -> createDBRecordUser(client, finalUser, finalNick)));
                                                    }
                                                    return event.reply(" ").withEphemeral(true);

                                                });
                                    else
                                        return event.reply("не магъэу робота полный рост гибрид");
                                })
                        );
            }
            return Mono.empty();
        }).subscribe();

        client.on(BanEvent.class, event -> {
            String userId = event.getUser().getId().asString();

            return Mono.fromCallable(() -> isUserExists(client, userId))
                    .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                    .flatMap(exists -> {
                        if (exists) {
                            return Mono.fromRunnable(() -> {
                                        try {
                                            db.removeId(userId);
                                        } catch (SQLException e) {
                                            throw new RuntimeException(e);
                                        }
                                    })
                                    .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                                    .then(
                                            client.getChannelById(debuggingChannelId)
                                                    .ofType(GuildMessageChannel.class)
                                                    .flatMap(channel -> channel.createMessage(EmbedCreateSpec.builder()
                                                            .title("шел плохой мозги ба")
                                                            .description(userId)
                                                            .build()))
                                    );
                        }
                        return Mono.empty();
                    });
        }).subscribe();
    }

    public Mono<Void> callEditUsersMenu(GatewayDiscordClient client, ChatInputInteractionEvent event) {
        return client.getGuildById(guildId)
                .flatMapMany(Guild::getMembers)
                .flatMap(member -> {
                    log.debug("CHECKING CHECKING CHECKING для пользователя: {}", member.getDisplayName());


                    String userId = member.getId().asString();
                    String displayName = member.getDisplayName();

                    if (displayName.contains("_") && isUserExists(client, userId) && member.isBot()) {
                        log.debug("checked");
                        users.put(userId, displayName);
                    } else if(!member.isBot()) {
                        log.debug("checked");
                        usersIdxToCheck.add(userId);
                    }

                    return Mono.empty();
                })
                .then()
                .then(
                        client.getChannelById(managingChannelId)
                                .ofType(MessageChannel.class)
                                .flatMapMany(channel -> {
                                            List<SelectMenu.DefaultValue> optionsList = new ArrayList<>(List.of());
                                            Flux.fromIterable(usersIdxToCheck)
                                                    .flatMap(user -> {
                                                        optionsList.add(SelectMenu.DefaultValue.of(Snowflake.of(user), SelectMenu.DefaultValue.Type.USER));
                                                        return Mono.empty();
                                                    });
                                            return event.editReply()
                                                    .withEmbeds(EmbedCreateSpec.builder().title("кьо это такие ба?").build())
                                                    .withComponents(ActionRow.of(SelectMenu.ofUser("menu-edit-nicks-users", optionsList)));
                                        }
                                )
                                .then()
                );
    }

    public Mono<Void> createDBRecordUser(GatewayDiscordClient client, String user, String nick) {
        try {
            db.setData(user, nick);
            return Mono.empty();
        } catch (SQLException e) {
            log.error(e.getMessage());
            return client.getChannelById(debuggingChannelId)
                    .ofType(GuildMessageChannel.class)
                    .flatMap(channel -> channel.createMessage(EmbedCreateSpec.builder()
                            .title("ашибка дон")
                            .description("иди проверь администрирование гибрида ба")
                            .build())
                    ).then();
        }
    }

    public boolean isUserExists(GatewayDiscordClient client, String user) {
        try {
            return db.isUserIdExists(user);
        } catch (SQLException e) {
            log.error("Ошибка проверки юзера: " + e.getMessage());
            return false;
        }
    }

    public String getUserNameFromDB(String id) {
        try {
            return db.getUserNickname(id);
        } catch (SQLException e) {
            log.error("Ошибка проверки юзера: " + e.getMessage());
            return "err";
        }
    }
}