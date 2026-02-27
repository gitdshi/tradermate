import os
import sys
from pathlib import Path

# Ensure repo root is on sys.path so local adapters (vnpy_mysql) are importable
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load simple .env file (if present) to populate env vars for vnpy
env_path = ROOT.joinpath('.env')
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        k, v = line.split('=', 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k not in os.environ:
            os.environ[k] = v

# Apply VN_DATABASE_* env to vnpy settings early so get_database can find adapter
try:
    from vnpy.trader import setting as vnsetting
    # Note: vnpy automatically prefixes 'vnpy_' to the database driver name
    # So 'mysql' becomes 'vnpy_mysql', 'sqlite' becomes 'vnpy_sqlite'
    vnsetting.SETTINGS['database.name'] = os.getenv('VN_DATABASE_NAME', os.getenv('MYSQL_DB_DRIVER', 'mysql'))
    vnsetting.SETTINGS['database.host'] = os.getenv('VN_DATABASE_HOST', os.getenv('MYSQL_HOST', '127.0.0.1'))
    vnsetting.SETTINGS['database.port'] = int(os.getenv('VN_DATABASE_PORT', os.getenv('MYSQL_PORT', '3306')))
    vnsetting.SETTINGS['database.user'] = os.getenv('VN_DATABASE_USER', os.getenv('MYSQL_USER', 'root'))
    vnsetting.SETTINGS['database.password'] = os.getenv('VN_DATABASE_PASSWORD') or os.getenv('MYSQL_PASSWORD')
    if not vnsetting.SETTINGS['database.password']:
        raise ValueError("Database password must be set via VN_DATABASE_PASSWORD or MYSQL_PASSWORD")
    vnsetting.SETTINGS['database.database'] = os.getenv('VN_DATABASE_DB', os.getenv('MYSQL_DATABASE', 'vnpy'))
except Exception:
    pass

# Configure datafeed from env (supports VN_DATAFEED_* or TUSHARE_TOKEN)
try:
    # prefer explicit VN_DATAFEED_NAME, else use 'tushare' when TUSHARE_TOKEN present
    # Note: vnpy automatically prefixes 'vnpy_' to the datafeed name
    datafeed_name = os.getenv('VN_DATAFEED_NAME')
    if not datafeed_name:
        if os.getenv('TUSHARE_TOKEN'):
            datafeed_name = 'tushare'
        else:
            datafeed_name = ''

    vnsetting.SETTINGS['datafeed.name'] = datafeed_name
    if datafeed_name:
        vnsetting.SETTINGS['datafeed.username'] = os.getenv('VN_DATAFEED_USERNAME', os.getenv('TUSHARE_TOKEN', ''))
        vnsetting.SETTINGS['datafeed.password'] = os.getenv('VN_DATAFEED_PASSWORD', '')
except Exception:
    pass

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
# Note: vnpy_ctp gateway is not compatible with macOS
# from vnpy_ctp import CtpGateway
from vnpy_ctastrategy import CtaStrategyApp
from vnpy_ctabacktester import CtaBacktesterApp
from vnpy_datamanager import DataManagerApp


def main():
    """Start VeighNa Trader"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

    # Note: CTP gateway removed due to macOS incompatibility
    # main_engine.add_gateway(CtpGateway)
    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(DataManagerApp)

    # Register custom CTA strategies (must be done BEFORE creating main_window)
    try:
        cta_engine = main_engine.get_engine("CtaStrategy")
        if cta_engine:
            # Load from project folder
            cta_engine.load_strategy_class_from_module("app.strategies.turtle_trading")
            cta_engine.load_strategy_class_from_module("app.strategies.triple_ma_strategy")
            
            # Also load from vntrader strategies folder and project-level strategies
            from pathlib import Path
            candidate_folders = [Path.home() / ".vntrader" / "strategies", ROOT / "strategies"]
            import os
            print(f"Process cwd: {os.getcwd()}")
            print(f"Path.cwd(): {Path.cwd()}")
            # Also show what Path.cwd()/strategies contains
            cwd_strat = Path.cwd() / "strategies"
            print(f"Project-level strategies path: {ROOT / 'strategies'} exists={ (ROOT / 'strategies').exists() }")
            print(f"User vntrader strategies path: {Path.home() / '.vntrader' / 'strategies'} exists={(Path.home() / '.vntrader' / 'strategies').exists()} ")
            print(f"Cwd strategies path: {cwd_strat} exists={cwd_strat.exists()}")
            if cwd_strat.exists():
                try:
                    print("Files in cwd strategies:", [p.name for p in cwd_strat.iterdir() if p.is_file()])
                except Exception:
                    pass

            for strategies_folder in candidate_folders:
                if strategies_folder.exists():
                    try:
                        print(f"Loading strategies from {strategies_folder}")
                        cta_engine.load_strategy_class_from_folder(strategies_folder)
                    except Exception:
                        print(f"Failed to load strategies from {strategies_folder}")
                        import traceback
                        traceback.print_exc()

            print(f"Loaded strategies: {cta_engine.get_all_strategy_class_names()}")
    except Exception as e:
        print(f"Failed to load custom strategies: {e}")
        import traceback
        traceback.print_exc()

    # Start a background thread to watch local strategy files and sync + reload engines
    try:
        import threading
        import time
        import shutil

        def watch_and_sync():
            watch_paths = [ROOT / "app" / "strategies"]
            user_strat = Path.home() / ".vntrader" / "strategies"
            project_strat = ROOT / "strategies"

            # Ensure target folders exist
            user_strat.mkdir(parents=True, exist_ok=True)
            project_strat.mkdir(parents=True, exist_ok=True)

            mtimes = {}

            while True:
                changed = False
                for p in watch_paths:
                    if not p.exists():
                        continue
                    for f in p.glob('*.py'):
                        try:
                            m = f.stat().st_mtime
                        except Exception:
                            continue
                        key = str(f.resolve())
                        if key not in mtimes or mtimes[key] != m:
                            mtimes[key] = m
                            changed = True
                            # copy to project and user strategy folders
                            try:
                                shutil.copy2(f, project_strat / f.name)
                                shutil.copy2(f, user_strat / f.name)
                                print(f"Synced strategy {f.name} to {project_strat} and {user_strat}")
                            except Exception:
                                print(f"Failed to copy strategy {f}")
                if changed:
                    try:
                        # reload cta strategy engine classes
                        if main_engine:
                            ce = main_engine.get_engine("CtaStrategy")
                            if ce:
                                if hasattr(ce, 'reload_strategy_class'):
                                    ce.reload_strategy_class()
                                else:
                                    # fallback: re-load folders
                                    for folder in (user_strat, project_strat):
                                        try:
                                            ce.load_strategy_class_from_folder(folder)
                                        except Exception:
                                            pass

                            be = main_engine.get_engine("CtaBacktester")
                            if be:
                                if hasattr(be, 'reload_strategy_class'):
                                    be.reload_strategy_class()
                                else:
                                    try:
                                        be.load_strategy_class()
                                    except Exception:
                                        pass
                        print("Strategy engines reloaded after sync")
                    except Exception:
                        import traceback
                        traceback.print_exc()

                time.sleep(2)

        t = threading.Thread(target=watch_and_sync, daemon=True)
        t.start()
    except Exception:
        pass

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()

if __name__ == "__main__":
    main()