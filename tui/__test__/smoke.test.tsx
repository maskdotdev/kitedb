import { test, expect } from "bun:test";
import { testRender } from "@opentui/solid";
import { App } from "../src/app.tsx";

test("renders base panes", async () => {
  const { renderOnce, captureCharFrame } = await testRender(() => <App />);
  await renderOnce();
  const frame = captureCharFrame();
  expect(frame).toContain("KiteDB Explorer");
  expect(frame).toContain("Nodes");
  expect(frame).toContain("Details");
});
