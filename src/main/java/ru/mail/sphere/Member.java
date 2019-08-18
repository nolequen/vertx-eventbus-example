package ru.mail.sphere;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class Member extends AbstractVerticle {

  public static void main(String[] args) {
    Vertx.clusteredVertx(new VertxOptions(), vertx -> vertx.result().deployVerticle(new Member()));
  }

  @Override
  public void start() {
    vertx.sharedData().getCounter("members", counter -> {
      if (counter.succeeded()) {
        counter.result().incrementAndGet(id -> enterParty(id.result()));
      }
    });
  }

  private void enterParty(long id) {
    final String name = "member#" + id;
    final JsonObject message = new JsonObject().put("name", name);
    System.out.println(name + " wants to join the party");
    vertx.eventBus().send("party.join", message);

    vertx.eventBus().<String>consumer(name, event -> {
          if (ThreadLocalRandom.current().nextBoolean()) {
            event.reply("Thanks!");
          }
        }
    );

    vertx.eventBus().<String>consumer("congratulations", event -> {
      final String to = event.body();
      if (to.equals(name)) {
        System.out.println("Yippee!");
      } else {
        sendCongratulations(to);
      }
    });
  }

  private void sendCongratulations(@NotNull String to) {
    final DeliveryOptions options = new DeliveryOptions().setSendTimeout(1000);
    vertx.eventBus().request(to, "Let's groove!", options, reply -> {
      if (reply.succeeded()) {
        System.out.println("Hangs out with " + to);
      } else {
        System.out.println("Bored of " + to);
      }
    });
  }
}
