/**
 * EdOpt Chat Widget
 * Embeddable chatbot for EdOpt.org
 *
 * Usage: <script src="https://chatbot.edopt.org/widget.js" defer></script>
 */
(function () {
  "use strict";

  // Configuration
  const API_BASE =
    document.currentScript?.getAttribute("data-api") ||
    (window.location.hostname === "localhost"
      ? "http://localhost:5012"
      : "https://chatbot.edopt.org");
  const STORAGE_KEY = "edopt_chat_session_id";
  const SESSION_TTL = 24 * 60 * 60 * 1000; // 24 hours

  let sessionId = null;
  let isOpen = false;
  let isLoading = false;
  let hasGreeted = false;

  // Load CSS
  function loadCSS() {
    if (document.getElementById("edopt-chat-css")) return;
    const link = document.createElement("link");
    link.id = "edopt-chat-css";
    link.rel = "stylesheet";
    link.href = API_BASE + "/widget.css";
    document.head.appendChild(link);
  }

  // Session management
  function getSession() {
    try {
      const data = JSON.parse(localStorage.getItem(STORAGE_KEY));
      if (data && Date.now() - data.ts < SESSION_TTL) {
        return data.id;
      }
    } catch (e) {}
    return null;
  }

  function saveSession(id) {
    try {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({ id: id, ts: Date.now() })
      );
    } catch (e) {}
  }

  // Simple Markdown → HTML converter
  function renderMarkdown(text) {
    if (!text) return "";
    let html = text
      // Escape HTML
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      // Headers
      .replace(/^### (.+)$/gm, "<h3>$1</h3>")
      .replace(/^## (.+)$/gm, "<h2>$1</h2>")
      .replace(/^# (.+)$/gm, "<h1>$1</h1>")
      // Bold + italic
      .replace(/\*\*\*(.+?)\*\*\*/g, "<strong><em>$1</em></strong>")
      // Bold
      .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
      // Italic
      .replace(/\*(.+?)\*/g, "<em>$1</em>")
      // Links [text](url)
      .replace(
        /\[([^\]]+)\]\(([^)]+)\)/g,
        '<a href="$2" target="_blank" rel="noopener">$1</a>'
      )
      // Inline code
      .replace(/`([^`]+)`/g, "<code>$1</code>")
      // Unordered lists
      .replace(/^- (.+)$/gm, "<li>$1</li>")
      // Line breaks
      .replace(/\n\n/g, "</p><p>")
      .replace(/\n/g, "<br>");

    // Wrap list items in <ul>
    html = html.replace(
      /(<li>.*?<\/li>)(?:\s*<br>)?/gs,
      function (match) {
        return match;
      }
    );
    // Group consecutive <li> elements into <ul>
    html = html.replace(
      /(?:<br>)?(<li>[\s\S]*?<\/li>)(?:<br>)?/g,
      "$1"
    );
    let inList = false;
    const lines = html.split(/(<li>|<\/li>)/);
    let result = "";
    for (let i = 0; i < lines.length; i++) {
      if (lines[i] === "<li>") {
        if (!inList) {
          result += "<ul>";
          inList = true;
        }
        result += "<li>";
      } else if (lines[i] === "</li>") {
        result += "</li>";
        // Check if next meaningful content is another <li>
        let nextLi = false;
        for (let j = i + 1; j < lines.length; j++) {
          const trimmed = lines[j].replace(/<br>/g, "").trim();
          if (trimmed === "<li>") {
            nextLi = true;
            break;
          }
          if (trimmed) break;
        }
        if (!nextLi && inList) {
          result += "</ul>";
          inList = false;
        }
      } else {
        result += lines[i];
      }
    }
    if (inList) result += "</ul>";

    return "<p>" + result + "</p>";
  }

  // Build DOM
  function buildWidget() {
    // Floating button
    const btn = document.createElement("button");
    btn.className = "edopt-chat-button";
    btn.setAttribute("aria-label", "Open EdOpt chat");
    btn.innerHTML = `<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
      <path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z"/>
    </svg>`;
    btn.onclick = togglePanel;

    // Chat panel
    const panel = document.createElement("div");
    panel.className = "edopt-chat-panel";
    panel.id = "edopt-chat-panel";
    panel.innerHTML = `
      <div class="edopt-chat-header">
        <div class="edopt-chat-header-title">
          <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
            <path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z"/>
          </svg>
          EdOpt Assistant
        </div>
        <button class="edopt-chat-close" aria-label="Close chat">&times;</button>
      </div>
      <div class="edopt-chat-messages" id="edopt-messages"></div>
      <div class="edopt-chat-input-area">
        <textarea class="edopt-chat-input" id="edopt-input"
          placeholder="Type your question..."
          rows="1"></textarea>
        <button class="edopt-chat-send" id="edopt-send" aria-label="Send message">
          <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
            <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/>
          </svg>
        </button>
      </div>
      <div class="edopt-chat-footer">
        Powered by <a href="https://edopt.org" target="_blank">EdOpt.org</a>
      </div>
    `;

    document.body.appendChild(btn);
    document.body.appendChild(panel);

    // Event listeners
    panel.querySelector(".edopt-chat-close").onclick = togglePanel;

    const input = document.getElementById("edopt-input");
    const sendBtn = document.getElementById("edopt-send");

    sendBtn.onclick = sendMessage;
    input.addEventListener("keydown", function (e) {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
      }
    });

    // Auto-resize textarea
    input.addEventListener("input", function () {
      this.style.height = "auto";
      this.style.height = Math.min(this.scrollHeight, 80) + "px";
    });
  }

  function togglePanel() {
    isOpen = !isOpen;
    const panel = document.getElementById("edopt-chat-panel");
    if (isOpen) {
      panel.classList.add("edopt-open");
      if (!hasGreeted) {
        fetchGreeting();
      }
      document.getElementById("edopt-input").focus();
    } else {
      panel.classList.remove("edopt-open");
    }
  }

  function addMessage(content, role) {
    const messages = document.getElementById("edopt-messages");
    const div = document.createElement("div");
    div.className = "edopt-msg edopt-msg-" + role;
    if (role === "assistant") {
      div.innerHTML = renderMarkdown(content);
    } else {
      div.textContent = content;
    }
    messages.appendChild(div);
    messages.scrollTop = messages.scrollHeight;
  }

  function showTyping() {
    const messages = document.getElementById("edopt-messages");
    const div = document.createElement("div");
    div.className = "edopt-typing";
    div.id = "edopt-typing-indicator";
    div.innerHTML =
      '<div class="edopt-typing-dot"></div>' +
      '<div class="edopt-typing-dot"></div>' +
      '<div class="edopt-typing-dot"></div>';
    messages.appendChild(div);
    messages.scrollTop = messages.scrollHeight;
  }

  function hideTyping() {
    const el = document.getElementById("edopt-typing-indicator");
    if (el) el.remove();
  }

  async function fetchGreeting() {
    hasGreeted = true;
    showTyping();
    try {
      const resp = await fetch(API_BASE + "/greet");
      const data = await resp.json();
      sessionId = data.session_id;
      saveSession(sessionId);
      hideTyping();
      addMessage(data.answer, "assistant");
    } catch (e) {
      hideTyping();
      addMessage(
        "Welcome! I'm the EdOpt Assistant. How can I help you explore education options in New Hampshire?",
        "assistant"
      );
    }
  }

  async function sendMessage() {
    const input = document.getElementById("edopt-input");
    const sendBtn = document.getElementById("edopt-send");
    const message = input.value.trim();
    if (!message || isLoading) return;

    // Show user message
    addMessage(message, "user");
    input.value = "";
    input.style.height = "auto";

    // Restore session if needed
    if (!sessionId) {
      sessionId = getSession();
    }

    isLoading = true;
    sendBtn.disabled = true;
    showTyping();

    try {
      const resp = await fetch(API_BASE + "/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: message,
          session_id: sessionId,
        }),
      });

      if (!resp.ok) {
        throw new Error("API error: " + resp.status);
      }

      const data = await resp.json();
      sessionId = data.session_id;
      saveSession(sessionId);
      hideTyping();
      addMessage(data.answer, "assistant");
    } catch (e) {
      hideTyping();
      addMessage(
        "I'm sorry, I'm having trouble right now. Please try again in a moment.",
        "assistant"
      );
    } finally {
      isLoading = false;
      sendBtn.disabled = false;
      input.focus();
    }
  }

  // Initialize
  function init() {
    sessionId = getSession();
    loadCSS();
    buildWidget();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
