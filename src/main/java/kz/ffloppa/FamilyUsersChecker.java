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
import discord4j.core.object.entity.Member;
import discord4j.core.object.entity.channel.GuildMessageChannel;
import discord4j.core.object.entity.channel.MessageChannel;
import discord4j.core.spec.EmbedCreateSpec;
import discord4j.core.spec.InteractionPresentModalSpec;
import discord4j.core.spec.MessageCreateSpec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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
                return event.getUser().asMember(guildId)
                        .flatMap(member -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
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
                                        .flatMap(guild -> guild.getMemberById(Snowflake.of(finalUser)))
                                        .flatMap(member -> member.edit().withNicknameOrNull(finalNick))
                                        .flatMap(a -> createDBRecordUser(client, finalUser, finalNick));
                            }
                            return event.edit().withComponents(Collections.emptyList());
                        });
            }
            return Mono.empty();
        }).subscribe();

        client.on(BanEvent.class, event -> Mono.fromCallable(() -> {
                    db.removeId(String.valueOf(event.getUser().getId()));
                    return true;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then(client.getChannelById(debuggingChannelId)
                        .ofType(GuildMessageChannel.class)
                        .flatMap(channel -> channel.createMessage(EmbedCreateSpec.builder()
                                .title("удалил дон")
                                .description(event.getUser().getId().asString())
                                .build())
                        ))
                .onErrorResume(Exception.class, error -> {
                    log.error(String.valueOf(error));
                    return client.getChannelById(debuggingChannelId)
                            .ofType(GuildMessageChannel.class)
                            .flatMap(channel -> channel.createMessage(EmbedCreateSpec.builder()
                                    .title("ашибка дон")
                                    .description("иди проверь консоль дон")
                                    .build())
                            );
                })).subscribe();
    }

    public void callEditUsersMenu(GatewayDiscordClient client, ChatInputInteractionEvent event) {
        client.getGuildById(guildId)
                .flatMapMany(Guild::getMembers)
                .flatMap(member -> {
                    log.debug("CHECKING CHECKING CHECKING для пользователя: {}", member.getDisplayName());

                    return member.getRoles()
                            .any(role -> role.getId().asString().equals("1525814865413931038"))
                            .map(isBot -> {
                                String userId = member.getId().asString();
                                String displayName = member.getDisplayName();

                                if (displayName.contains("_") && isUserExists(client, userId) && isBot) {
                                    log.debug("checked");
                                    users.put(userId, displayName);
                                } else if(!isBot) {
                                    log.debug("checked");
                                    usersIdxToCheck.add(userId);
                                }
                                return member;
                            });
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
                                            return event.reply()
                                                    .withEmbeds(EmbedCreateSpec.builder().title("Кто? (на размышление дается 2 секунды)").build())
                                                    .withComponents(ActionRow.of(SelectMenu.ofUser("menu-edit-nicks-users", optionsList)))
                                                    .withEphemeral(true);
                                        }
                                )
                                .then()
                )
                .subscribe(
                        null,
                        error -> log.error("Ошибка при обработке пользователей гильдии: ", error),
                        () -> log.info("Проверка завершена! Сообщения успешно отправлены. Всего к проверке: {}", usersIdxToCheck.size())
                );
    }

    public Mono<Void> createDBRecordUser(GatewayDiscordClient client, String user, String nick) {
        return Mono.defer(() -> {
            try {
                db.setData(user, nick);
                return Mono.empty();
            } catch (SQLException e) {
                log.error(e.getMessage());
                return client.getChannelById(debuggingChannelId)
                        .ofType(GuildMessageChannel.class)
                        .flatMap(channel -> channel.createMessage(EmbedCreateSpec.builder()
                                .title("ашибка дон")
                                .description("иди проверь консоль дон")
                                .build())
                        );
            }
        }).then();
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