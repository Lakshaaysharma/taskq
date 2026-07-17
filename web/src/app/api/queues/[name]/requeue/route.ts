import { NextRequest, NextResponse } from "next/server";
import { requeue } from "@/lib/queue";

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ name: string }> },
) {
  try {
    const { name } = await params;
    const body = await req.json().catch(() => ({}));
    const from = body?.from === "scheduled" ? "scheduled" : "dead";
    const moved = await requeue(name, from);
    return NextResponse.json({ name, from, moved });
  } catch (e) {
    return NextResponse.json(
      { error: e instanceof Error ? e.message : "unknown error" },
      { status: 500 },
    );
  }
}
