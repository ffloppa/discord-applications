package kz.ffloppa;

import discord4j.common.util.Snowflake;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.guild.BanEvent;
import discord4j.core.event.domain.interaction.ModalSubmitInteractionEvent;
import discord4j.core.event.domain.interaction.SelectMenuInteractionEvent;
import discord4j.core.object.component.ActionRow;
import discord4j.core.object.component.SelectMenu;
import discord4j.core.object.component.TextInput;
import discord4j.core.object.entity.channel.GuildMessageChannel;
import discord4j.core.object.entity.channel.MessageChannel;
import discord4j.core.spec.EmbedCreateSpec;
import discord4j.core.spec.InteractionPresentModalSpec;
import discord4j.core.spec.MessageCreateSpec;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static kz.ffloppa.Main.*;

public class FamilyUsersChecker {
    private static final Logger log = Loggers.getLogger(Main.class);
    private final Database db = new Database();
    private final HashMap<String,String> users = new HashMap<>();
    private final Collection<String> usersIdxToCheck = new ArrayList<>();

    FamilyUsersChecker(GatewayDiscordClient client) {
        client.getGuildById(guildId)
              .subscribe(guild ->
                      guild.getMembers()
                           .map(member -> {
                               if(member.getDisplayName().contains("_") && isUserExists(client, member.getId().asString()))
                                  return Objects.requireNonNull(users.put(member.getId().asString(), member.getDisplayName()));
                               else
                                  return usersIdxToCheck.add(member.getId().asString());
                           })
              );

        client.getChannelById(managingChannelId)
                .ofType(MessageChannel.class)
                .subscribe(channel -> {
                    StringBuilder buttonId = new StringBuilder("edit-nick-");
                    for(String user : usersIdxToCheck)
                        buttonId.append(user);
                    for(String user : usersIdxToCheck) {
                        List<SelectMenu.DefaultValue> optionsList = new ArrayList<>();
                        for(String userIdx : usersIdxToCheck)
                            optionsList.add(SelectMenu.DefaultValue.of(Snowflake.of(userIdx), SelectMenu.DefaultValue.Type.USER));
                        SelectMenu selectMenu = SelectMenu.ofUser("menu-edit-nicks-users", optionsList);

                        channel.createMessage(MessageCreateSpec.builder()
                                .embeds(EmbedCreateSpec.builder().title("Кто из них кто?")
                                        .addField(String.format("<@%s>", user), "???", true)
                                        .build())
                                .components(ActionRow.of(selectMenu))
                                .build());
                    }
                });

        client.on(SelectMenuInteractionEvent.class, event -> {
            if (event.getCustomId().equals("menu-edit-nicks-users")) {

                List<String> selectedValues = event.getValues();
                String choice = selectedValues.getFirst();
                TextInput nickInput = TextInput.small("nickname", "Кьо этость?")
                        .placeholder(choice);
                return event.presentModal(InteractionPresentModalSpec.builder()
                        .title("выставь ник дон")
                        .customId("fill-user-nick")
                        .addComponent(ActionRow.of(nickInput))
                        .build()
                );
            }
            return Mono.empty();
        }).subscribe();

        client.on(ModalSubmitInteractionEvent.class, event -> {
            if (event.getCustomId().equals("fill-user-nick")) {
                AtomicBoolean isAdmin = new AtomicBoolean();
                event.getUser().asMember(guildId)
                        .flatMap(member -> member.getRoles().any(role -> role.getId().equals(adminRoleId)))
                        .subscribe(isAdmin::set);
                if(isAdmin.get()) {

                    String nick = "???";
                    String user = "???";
                    for (TextInput input : event.getComponents(TextInput.class)) {
                        if (input.getCustomId().equals("nickname")) {
                            nick = input.getPlaceholder().orElse("???");
                            user = input.getValue().orElse("???");
                        }
                    }

                    String finalUser = user;
                    String finalNick = nick;
                    return client.getGuildById(guildId)
                            .flatMap(guild -> guild.getMemberById(Snowflake.of(finalUser)))
                            .flatMap(member -> member.edit().name(finalNick))
                            .flatMap(_ -> createDBRecordUser(client, finalUser,finalNick));
                }
            }
            return Mono.empty();
        }).subscribe();

        client.on(BanEvent.class, event -> {
            // Wipe out user from DB
            return Mono.fromCallable(() -> {
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
                    });
        });
    }

    public Mono<Void> createDBRecordUser(GatewayDiscordClient client, String user, String nick) {
        return Mono.defer(() -> {
            try {
                db.setData(user,nick);
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
            }}).then();
    }

    public boolean isUserExists(GatewayDiscordClient client, String user) {
        AtomicBoolean bool = new AtomicBoolean();
        Mono.defer(() -> {
            try {
                bool.set(db.isUserIdExists(user));
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
            }}).then();
        return bool.get();
    }
}
