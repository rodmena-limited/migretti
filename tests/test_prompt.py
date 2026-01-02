from migretti.__main__ import cmd_prompt


def test_cmd_prompt(capsys):
    class Args:
        pass

    cmd_prompt(Args())
    captured = capsys.readouterr()
    assert "# Migretti - Database Migration Tool Guide" in captured.out
    assert "mg create <name>" in captured.out
