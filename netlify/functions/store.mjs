import { getStore } from "@netlify/blobs";

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

export default async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: cors });
  }

  const store = getStore("iv-shares");

  if (req.method === "POST") {
    const body = await req.text();
    if (!body) return new Response("empty body", { status: 400, headers: cors });
    const id = Math.random().toString(36).slice(2, 9);
    await store.set(id, body);
    return Response.json({ id }, { headers: cors });
  }

  if (req.method === "GET") {
    const id = new URL(req.url).searchParams.get("id");
    if (!id) return new Response("missing id", { status: 400, headers: cors });
    const val = await store.get(id);
    if (val === null) return new Response("not found", { status: 404, headers: cors });
    return new Response(val, { headers: { ...cors, "Content-Type": "text/plain" } });
  }

  return new Response("method not allowed", { status: 405, headers: cors });
};

export const config = { path: "/api/store" };
