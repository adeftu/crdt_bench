package de.oneandone.eventtracker.CRDT.ORset;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedHashMap;

import de.oneandone.eventtracker.CRDT.ORset.Stores.HeapStore;
import de.oneandone.eventtracker.CRDT.ORset.Stores.RedisStore;

@SuppressWarnings("rawtypes")
public class Console {
	private Client client = null;
	
	private abstract class Command {
		protected void checkArguments(int received, int required) throws RuntimeException {
			if (received != required)
				throw new RuntimeException("Invalid number of arguments!");
		}
		
		public abstract String run(String... args) throws RuntimeException;
		public abstract String help();
	}
	
	private LinkedHashMap<String, Command> commands = new LinkedHashMap<String, Command>();

	
	
	private class CommandHelp extends Command {
		public static final String NAME = "help";
		public static final String DESCRIPTION = "Print help message";
		
		@Override
		public String run(String... args) throws RuntimeException {
			checkArguments(args.length, 1);
			StringBuilder sb = new StringBuilder();
			Iterator<Command> i = commands.values().iterator();
			while (i.hasNext()) {
				sb.append(i.next().help());
				if (i.hasNext())
					sb.append("\n");
			}
			return sb.toString();
		}

		@Override
		public String help() {
			return String.format("%s\t\t\t\t\t: %s.", NAME, DESCRIPTION);
		}
		
	}
	
	private class CommandInit extends Command {
		public static final String NAME = "init";
		public static final String DESCRIPTION = "Select the type of store to use and set up the topology";
		
		@Override
		public String run(String... args) throws RuntimeException {
			checkArguments(args.length, 3);
			final String store = args[1];
			final String topologyFile = args[2];
			Client c;
			if (store.equals("heap")) {
				c = new Client<HeapStore>(HeapStore.class);
				try {
					HeapStore.clearAndSetTopology(topologyFile);
				} catch (Exception e) {
					throw new RuntimeException(e.getMessage());
				}
			}
			else if (store.equals("redis")) {
				c = new Client<RedisStore>(RedisStore.class);
				try {
					RedisStore.clearAndSetTopology(topologyFile);
				} catch (Exception e) {
					throw new RuntimeException(e.getMessage());
				}
			}
			else
				throw new RuntimeException("Invalid store type!");
			if (client != null) {
				client.clear();
				client.close();
			}
			client = c;
			try {
				Topology topology = new Topology();
				topology.loadFromFile(topologyFile);
				return String.format("Topology:\n%s\nUsing TTL of %d ms\nInitialization successful", topology.toString(), Element.getTTL());
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage());
			}
		}

		@Override
		public String help() {
			return String.format("%s <heap|redis> <topology XML file>\t: %s.", NAME, DESCRIPTION);
		}
	}
	
	private class CommandExit extends Command {
		public static final String NAME = "exit";
		public static final String DESCRIPTION = "Terminate the application";
		
		@Override
		public String run(String... args) throws RuntimeException {
			checkArguments(args.length, 1);
			if (client != null) {
				client.clear();
				client.close();
			}
			return "Bye!";
		}

		@Override
		public String help() {
			return String.format("%s\t\t\t\t\t: %s.", NAME, DESCRIPTION);
		}
	}

	private class CommandBoot extends Command {
		public static final String NAME = "boot";
		public static final String DESCRIPTION = "Connect the client to the cluster through a bootstrap store";
		
		@Override
		public String run(String... args) throws RuntimeException {
			checkArguments(args.length, 3);
			final String host = args[1];
			final int port = Integer.parseInt(args[2]);
			if (client != null)
				client.close();
			client.boot(host, port);
			return "Booted successfully";
		}

		@Override
		public String help() {
			return String.format("%s <host> <port>\t\t\t: %s.", NAME, DESCRIPTION);
		}
	}
	
	private class CommandTTL extends Command {
		public static final String NAME = "ttl";
		public static final String DESCRIPTION = "Set TTL in milliseconds of each element";
		
		@Override
		public String run(String... args) throws RuntimeException {
			checkArguments(args.length, 2);
			final int ttl = Integer.parseInt(args[1]);
			Element.setTTL(ttl);
			return "TTL set";
		}

		@Override
		public String help() {
			return String.format("%s <milliseconds>\t\t\t: %s.", NAME, DESCRIPTION);
		}
	}
	
	private class CommandAdd extends Command {
		public static final String NAME = "add";
		public static final String DESCRIPTION = "Add a value to the set";
		
		@Override
		public String run(String... args) throws RuntimeException {
			checkArguments(args.length, 2);
			final String value = args[1];
			if (client == null)
				throw new RuntimeException("You have to boot first!");
			client.add(value);
			return "Value added";
		}

		@Override
		public String help() {
			return String.format("%s <value>\t\t\t\t: %s.", NAME, DESCRIPTION);
		}
	}
	
	private class CommandRemove extends Command {
		public static final String NAME = "remove";
		public static final String DESCRIPTION = "Remove a value from the set";
		
		@Override
		public String run(String... args) throws RuntimeException {
			checkArguments(args.length, 2);
			final String value = args[1];
			client.remove(value);
			return "Value removed";
		}

		@Override
		public String help() {
			return String.format("%s <value>\t\t\t\t: %s.", NAME, DESCRIPTION);
		}
	}
	
	private class CommandLookup extends Command {
		public static final String NAME = "lookup";
		public static final String DESCRIPTION = "Search a value in the set";
		
		@Override
		public String run(String... args) throws RuntimeException {
			checkArguments(args.length, 2);
			final String value = args[1];
			final boolean found = client.lookup(value);
			return String.format("Value %sfound", found ? "" : "not ");
		}

		@Override
		public String help() {
			return String.format("%s <value>\t\t\t\t: %s.", NAME, DESCRIPTION);
		}
	}
	
	private class CommandPull extends Command {
		public static final String NAME = "pull";
		public static final String DESCRIPTION = "Pull updates from another cluster";
		
		@Override
		public String run(String... args) throws RuntimeException {
			checkArguments(args.length, 2);
			final String rc = args[1];
			client.pullUpdates(rc);
			return "Updates pulled";
		}

		@Override
		public String help() {
			return String.format("%s <cluster ID>\t\t\t: %s.", NAME, DESCRIPTION);
		}
	}
	
	public Console() {
		commands.put(CommandHelp.NAME, new CommandHelp());
		commands.put(CommandInit.NAME, new CommandInit());
		commands.put(CommandTTL.NAME, new CommandTTL());
		commands.put(CommandBoot.NAME, new CommandBoot());
		commands.put(CommandAdd.NAME, new CommandAdd());
		commands.put(CommandRemove.NAME, new CommandRemove());
		commands.put(CommandLookup.NAME, new CommandLookup());
		commands.put(CommandPull.NAME, new CommandPull());
		commands.put(CommandExit.NAME, new CommandExit());
	}
	
	public void run() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			try {
				System.out.print("> ");
				System.out.flush();
				String[] cmdTokens = br.readLine().split(" ");
				if (commands.containsKey(cmdTokens[0])) {
					System.out.println(commands.get(cmdTokens[0]).run(cmdTokens));
					if (cmdTokens[0].equals(CommandExit.NAME))
						break;
				}
				else {
					System.err.println("Invalid command!");
					System.out.println(commands.get(CommandHelp.NAME).run((String[])null));
				}
			} catch (Exception e) {
				if (e.getMessage() != null)
					System.err.println(e.getMessage());
			}
		}
	}
	
	public static void main(String[] args) {
		new Console().run();
	}
}
