package ru.mail.sphere;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class Party extends AbstractVerticle {

  private final @NotNull Duration period = Duration.ofSeconds(30);

  public static void main(String[] args) {
    Vertx.clusteredVertx(new VertxOptions(), vertx -> vertx.result().deployVerticle(new Party()));
  }

  @Override
  public void start(@NotNull Promise<Void> startPromise) {
    final LocalMap<String, JsonObject> members = vertx.sharedData().getLocalMap("partyMembers");

    vertx.setPeriodic(period.toMillis(), timer -> {
      final String randomMember = new ArrayList<>(members.keySet()).get(ThreadLocalRandom.current().nextInt(members.size()));
      System.out.println("congratulations to " + randomMember);
      vertx.eventBus().publish("congratulations", randomMember);
    });

    vertx.eventBus().<JsonObject>consumer("party.join", event -> {
      final JsonObject member = event.body();
      final String name = member.getString("name");
      System.out.println(name + " joins the party");
      members.putIfAbsent(name, member);
    }).completionHandler(startPromise);
  }
}
