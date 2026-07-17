import { NextRequest, NextResponse } from "next/server";
import { getTask } from "@/lib/queue";

export const dynamic = "force-dynamic";

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  try {
    const { id } = await params;
    const task = await getTask(id);
    if (!task) {
      return NextResponse.json({ error: "not found" }, { status: 404 });
    }
    return NextResponse.json({ task });
  } catch (e) {
    return NextResponse.json(
      { error: e instanceof Error ? e.message : "unknown error" },
      { status: 500 },
    );
  }
}
