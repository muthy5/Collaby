import "dotenv/config";
import Anthropic from "@anthropic-ai/sdk";
import { execSync } from "child_process";
import { readFileSync, writeFileSync, readdirSync, existsSync } from "fs";
import { bridge, messageQueue } from "./bridge_server.js";

if (!process.env.ANTHROPIC_API_KEY) {
  console.error("❌ ANTHROPIC_API_KEY not set. Copy .env.example to .env and add your key.");
  process.exit(1);
}

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const tools = [
  {
    name: "run_command",
    description: "Run a shell command and return output. Use this to start scrape.py or any other command.",
    input_schema: {
      type: "object",
      properties: { command: { type: "string" } },
      required: ["command"]
    }
  },
  {
    name: "read_file",
    description: "Read a file from disk",
    input_schema: {
      type: "object",
      properties: { path: { type: "string" } },
      required: ["path"]
    }
  },
  {
    name: "write_file",
    description: "Write content to a file on disk",
    input_schema: {
      type: "object",
      properties: {
        path: { type: "string" },
        content: { type: "string" }
      },
      required: ["path", "content"]
    }
  },
  {
    name: "list_dir",
    description: "List files in a directory",
    input_schema: {
      type: "object",
      properties: { path: { type: "string" } },
      required: ["path"]
    }
  },
  {
    name: "report_to_brain",
    description: "Claude Code reports back to Claude with results, observations, or questions",
    input_schema: {
      type: "object",
      properties: { message: { type: "string" } },
      required: ["message"]
    }
  },
  {
    name: "check_scraper_messages",
    description: "Check for new messages from scrape.py via the bridge. Returns queued messages (captcha alerts, scraper failures, login issues, status updates, completion summaries). Call this periodically while scrape.py is running.",
    input_schema: {
      type: "object",
      properties: {},
      required: []
    }
  },
  {
    name: "respond_to_scraper",
    description: "Send a response back to scrape.py via the bridge. Use after receiving a scraper message that needs a decision. Actions: 'heal' (with instructions), 'skip' (with reason), 'retry' (with optional url/wait_ms), 'continue'.",
    input_schema: {
      type: "object",
      properties: {
        action: {
          type: "string",
          enum: ["heal", "skip", "retry", "continue"],
          description: "What scrape.py should do"
        },
        instructions: {
          type: "string",
          description: "Instructions for heal action (optional)"
        },
        reason: {
          type: "string",
          description: "Reason for skip action (optional)"
        },
        url: {
          type: "string",
          description: "Alternate URL for retry action (optional)"
        },
        wait_ms: {
          type: "number",
          description: "Wait time in ms before retry (optional)"
        }
      },
      required: ["action"]
    }
  }
];

async function executeTool(name, input) {
  if (name === "run_command")
    return execSync(input.command, { encoding: "utf8", timeout: 600000 });
  if (name === "read_file")
    return readFileSync(input.path, "utf8");
  if (name === "write_file") {
    writeFileSync(input.path, input.content);
    return `Written: ${input.path}`;
  }
  if (name === "list_dir")
    return readdirSync(input.path).join("\n");
  if (name === "report_to_brain")
    return `Brain received: ${input.message}`;
  if (name === "check_scraper_messages") {
    if (messageQueue.length === 0)
      return "No new messages from scraper.";
    const msgs = messageQueue.splice(0, messageQueue.length);
    return JSON.stringify(msgs, null, 2);
  }
  if (name === "respond_to_scraper") {
    const sent = bridge.respond(input);
    return sent ? `Response sent to scraper: ${JSON.stringify(input)}` : "Failed: scraper not connected";
  }
  return "Unknown tool";
}

const MAX_ITERATIONS = 50;

async function run(goal) {
  console.log(`\n🧠 Brain received goal: ${goal}\n`);

  // Start the bridge server so scrape.py can connect
  try {
    await bridge.start();
  } catch (e) {
    console.log(`⚠️ Bridge server failed to start: ${e.message} — continuing without bridge`);
  }

  const messages = [{ role: "user", content: goal }];
  let iteration = 0;

  while (iteration < MAX_ITERATIONS) {
    iteration++;
    const response = await client.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 4096,
      system: `You are the brain in a brain-hands system. Claude Code is your hands.
- You decide WHAT to do and WHY
- You use tools to instruct Claude Code to act
- Claude Code executes and reports back via report_to_brain
- You keep looping until the goal is complete
- Think out loud so Claude Code understands your reasoning

BRIDGE SYSTEM: A TCP bridge connects you to scrape.py in real time.
- scrape.py sends messages when it hits CAPTCHAs, login failures, or 0 results
- Use check_scraper_messages to see what scrape.py needs
- Use respond_to_scraper to tell it what to do (heal, skip, retry, continue)
- When running scrape.py, periodically check for messages while it's executing

IMPORTANT: When running scrape.py, use run_command with a long timeout. The scraper takes 10-30 minutes.`,
      tools,
      messages
    });

    for (const block of response.content) {
      if (block.type === "text") {
        console.log(`\n🧠 Brain: ${block.text}`);
      }
    }

    if (response.stop_reason === "end_turn") {
      console.log("\n✅ Goal complete.");
      break;
    }

    if (response.stop_reason === "tool_use") {
      const toolResults = [];

      for (const block of response.content) {
        if (block.type !== "tool_use") continue;

        console.log(`\n🤝 Hands executing: ${block.name}`);
        console.log(`   Input: ${JSON.stringify(block.input).slice(0, 300)}`);

        let result;
        try {
          result = await executeTool(block.name, block.input);
          console.log(`✋ Hands report back: ${String(result).slice(0, 300)}`);
        } catch (err) {
          result = `ERROR: ${err.message}`;
          console.log(`❌ Hands error: ${err.message}`);
        }

        toolResults.push({
          type: "tool_result",
          tool_use_id: block.id,
          content: String(result).slice(0, 10000)
        });
      }

      messages.push({ role: "assistant", content: response.content });
      messages.push({ role: "user", content: toolResults });
    }
  }

  if (iteration >= MAX_ITERATIONS) {
    console.log(`\n⚠️ Reached ${MAX_ITERATIONS} iterations. Stopping.`);
  }

  bridge.stop();
}

const goal = process.argv[2] || "Explore this codebase and give me a summary of what it does.";
run(goal);
