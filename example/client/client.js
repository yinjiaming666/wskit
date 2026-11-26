const byId = (id) => document.getElementById(id);
const user = byId("user");
const recipient = byId("recipient");
const channel = byId("channel");
const message = byId("text");
const log = byId("log");
let socket = null;

user.value = new URLSearchParams(location.search).get("user") || "alice";

function record(kind, text) {
  const line = document.createElement("span");
  line.textContent = kind + " · " + text;
  log.append(line);
  log.scrollTop = log.scrollHeight;
}

function setState(state) {
  const labels = { offline: "未连接", connecting: "连接中", online: "已连接", closing: "断开中" };
  byId("status").textContent = labels[state];
  byId("connect").disabled = state !== "offline";
  user.disabled = state !== "offline";
  byId("disconnect").disabled = state !== "online";
  byId("actions").disabled = state !== "online";
}

function send(op, target, text) {
  const data = JSON.stringify({ op, target, text });
  socket.send(data);
  record("发送", data);
}

byId("connection").addEventListener("submit", async (event) => {
  event.preventDefault();
  const userID = user.value;
  setState("connecting");
  try {
    const query = new URLSearchParams({ user: userID });
    const response = await fetch("/route?" + query, { signal: AbortSignal.timeout(8000) });
    if (!response.ok) throw new Error((await response.text()).trim() || response.statusText);
    const route = await response.json();
    socket = new WebSocket(route.ws_url);
    socket.onopen = () => {
      setState("online");
      record("连接", userID + " 已连接 " + route.endpoint_id + "，默认加入 lobby");
    };
    socket.onmessage = ({ data }) => {
      const { text } = JSON.parse(data);
      record("收到", text);
    };
    socket.onerror = () => record("错误", "WebSocket 连接出错");
    socket.onclose = () => {
      socket = null;
      setState("offline");
      record("连接", "已断开");
    };
  } catch (error) {
    setState("offline");
    record("错误", error.message);
  }
});

byId("disconnect").onclick = () => {
  setState("closing");
  socket.close(1000);
};
byId("join").onclick = () => send("join", channel.value);
byId("leave").onclick = () => send("leave", channel.value);
byId("direct").onclick = () => send("send", recipient.value, message.value);
byId("broadcast").onclick = () => send("sendToChannel", channel.value, message.value);
window.addEventListener("pagehide", () => socket?.close());
