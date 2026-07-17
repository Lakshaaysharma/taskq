import { NextRequest, NextResponse } from "next/server";
import { setPaused } from "@/lib/queue";

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ name: string }> },
) {
  try {
    const { name } = await params;
    const body = await req.json().catch(() => ({}));
    const paused = Boolean(body?.paused);
    await setPaused(name, paused);
    return NextResponse.json({ name, paused });
  } catch (e) {
    return NextResponse.json(
      { error: e instanceof Error ? e.message : "unknown error" },
      { status: 500 },
    );
  }
}
