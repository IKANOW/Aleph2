package com.ikanow.aleph2.data_import_manager.actors;

import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;
import akka.actor.Cancellable;
import akka.actor.UntypedActor;

public class FolderWatcherActor extends UntypedActor {

	private final Cancellable tick = getContext()
			.system()
			.scheduler()
			.schedule(Duration.create(500, TimeUnit.MILLISECONDS),
					Duration.create(1000, TimeUnit.MILLISECONDS), getSelf(),
					"tick", getContext().dispatcher(), null);

	@Override
	public void postStop() {
		tick.cancel();
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message.equals("tick")) {
			// do something useful here
		} else {
			unhandled(message);
		}
	}
}
