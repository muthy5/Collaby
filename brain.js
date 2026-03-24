import Anthropic from "@anthropic-ai/sdk";
import { execSync } from "child_process";
import { readFileSync, writeFileSync, readdirSync, existsSync } from "fs";

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const tools = [
  {
    name: "run_command",
    description: "Run a shell command and return output",
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
  }
];

async function executeTool(name, input) {
  if (name === "run_command")
    return execSync(input.command, { encoding: "utf8", timeout: 30000 });
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
  return "Unknown tool";
}

async function run(goal) {
  console.log(`\n🧠 Brain received goal: ${goal}\n`);
  const messages = [{ role: "user", content: goal }];

  while (true) {
    const response = await client.messages.create({
      model: "claude-opus-4-5",
      max_tokens: 4096,
      system: `You are the brain in a brain-hands system. Claude Code is your hands.
- You decide WHAT to do and WHY
- You use tools to instruct Claude Code to act
- Claude Code executes and reports back via report_to_brain
- You listen to those reports and decide next steps
- You keep looping until the goal is complete
- Think out loud so Claude Code understands your reasoning`,
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
        console.log(`   Input: ${JSON.stringify(block.input)}`);

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
          content: String(result)
        });
      }

      messages.push({ role: "assistant", content: response.content });
      messages.push({ role: "user", content: toolResults });
    }
  }
}

const goal = process.argv[2] || "Explore this codebase and give me a summary of what it does.";
run(goal);
