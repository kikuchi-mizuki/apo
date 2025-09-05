#!/usr/bin/env python3
"""
Googleカレンダー→AI抽出→スプレッドシート連携システム
メインCLIアプリケーション
"""
import click
import json
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger

from app.services.sync_service import CalendarSyncService
from app.core.config import config_manager


@click.group()
@click.version_option(version='1.0.0')
def cli():
    """Googleカレンダー→AI抽出→スプレッドシート連携システム"""
    pass


@cli.command()
@click.option('--past', default=30, help='過去何日分を取得するか（デフォルト: 30日）')
@click.option('--future', default=60, help='未来何日分を取得するか（デフォルト: 60日）')
@click.option('--start-date', help='開始日（YYYY-MM-DD形式）')
@click.option('--end-date', help='終了日（YYYY-MM-DD形式）')
@click.option('--dry-run', is_flag=True, help='実際の更新は行わず、処理内容のみ表示')
def sync(past, future, start_date, end_date, dry_run):
    """カレンダーからスプレッドシートへの同期を実行"""
    try:
        # 日付を設定
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            start_dt = datetime.now() - timedelta(days=past)
            end_dt = datetime.now() + timedelta(days=future)
        
        logger.info(f"同期開始: {start_dt.strftime('%Y-%m-%d')} 〜 {end_dt.strftime('%Y-%m-%d')}")
        
        if dry_run:
            logger.info("ドライラン実行（実際の更新は行いません）")
        
        # 同期サービスを初期化
        sync_service = CalendarSyncService()
        
        # 同期を実行
        result = sync_service.sync_calendar_to_sheets(start_dt, end_dt)
        
        # 結果を表示
        _display_sync_result(result)
        
        if result.errors > 0:
            click.echo(f"\n⚠️  エラーが{result.errors}件発生しました")
            for error in result.error_details[:5]:  # 最初の5件のみ表示
                click.echo(f"  - {error}")
        
        click.echo(f"\n✅ 同期完了: {result.upserted}件更新, {result.skipped}件スキップ")
        
    except Exception as e:
        logger.error(f"同期でエラーが発生しました: {e}")
        click.echo(f"❌ エラー: {e}")
        raise click.Abort()


@cli.command()
def status():
    """システムの状況を確認"""
    try:
        sync_service = CalendarSyncService()
        status_info = sync_service.get_sync_status()
        
        click.echo("🔍 システム状況")
        click.echo("=" * 50)
        
        # 全体状況
        status_emoji = "✅" if status_info['status'] == 'ready' else "❌"
        click.echo(f"状況: {status_emoji} {status_info['status']}")
        click.echo(f"最終チェック: {status_info['last_check']}")
        
        # カレンダー情報
        click.echo("\n📅 カレンダー")
        click.echo("-" * 20)
        calendar_info = status_info.get('calendar', {})
        if calendar_info.get('info'):
            info = calendar_info['info']
            click.echo(f"ID: {info.get('id', 'N/A')}")
            click.echo(f"タイトル: {info.get('summary', 'N/A')}")
            click.echo(f"タイムゾーン: {info.get('timeZone', 'N/A')}")
        
        calendar_permissions = calendar_info.get('permissions', False)
        click.echo(f"アクセス権限: {'✅ あり' if calendar_permissions else '❌ なし'}")
        
        # スプレッドシート情報
        click.echo("\n📊 スプレッドシート")
        click.echo("-" * 20)
        sheet_info = status_info.get('spreadsheet', {})
        if sheet_info.get('info'):
            info = sheet_info['info']
            click.echo(f"シート名: {info.get('title', 'N/A')}")
            click.echo(f"行数: {info.get('row_count', 'N/A')}")
            click.echo(f"データ行数: {info.get('data_rows', 'N/A')}")
        
        sheet_permissions = sheet_info.get('permissions', False)
        click.echo(f"アクセス権限: {'✅ あり' if sheet_permissions else '❌ なし'}")
        
        # 設定情報
        click.echo("\n⚙️  設定")
        click.echo("-" * 20)
        config_info = status_info.get('config', {})
        click.echo(f"カレンダーID: {config_info.get('calendar_id', 'N/A')}")
        click.echo(f"シート名: {config_info.get('sheet_name', 'N/A')}")
        click.echo(f"【B】パターン: {config_info.get('b_event_pattern', 'N/A')}")
        click.echo(f"信頼度閾値: {config_info.get('confidence_threshold', 'N/A')}")
        click.echo(f"AIプロバイダー: {config_info.get('ai_provider', 'N/A')}")
        click.echo(f"AIモデル: {config_info.get('ai_model', 'N/A')}")
        
    except Exception as e:
        logger.error(f"状況確認でエラーが発生しました: {e}")
        click.echo(f"❌ エラー: {e}")
        raise click.Abort()


@cli.command()
@click.option('--days', default=90, help='何日前までのレコードを保持するか（デフォルト: 90日）')
@click.option('--confirm', is_flag=True, help='確認なしで実行')
def cleanup(days, confirm):
    """古いレコードをクリーンアップ"""
    try:
        if not confirm:
            click.echo(f"⚠️  過去{days}日より古いレコードを削除します")
            if not click.confirm("続行しますか？"):
                click.echo("キャンセルしました")
                return
        
        sync_service = CalendarSyncService()
        result = sync_service.cleanup_old_records(days)
        
        click.echo(f"🧹 クリーンアップ完了")
        click.echo(f"削除件数: {result['removed']}件")
        click.echo(f"チェック件数: {result['total_checked']}件")
        
        if 'error' in result:
            click.echo(f"⚠️  エラー: {result['error']}")
        
    except Exception as e:
        logger.error(f"クリーンアップでエラーが発生しました: {e}")
        click.echo(f"❌ エラー: {e}")
        raise click.Abort()


@cli.command()
@click.option('--format', 'format_type', default='csv', type=click.Choice(['csv', 'json']), help='出力フォーマット')
@click.option('--output', help='出力ファイルパス（指定しない場合は標準出力）')
def export(format_type, output):
    """データをエクスポート"""
    try:
        sync_service = CalendarSyncService()
        data = sync_service.export_data(format_type)
        
        if output:
            # ファイルに出力
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(data)
            
            click.echo(f"✅ データをエクスポートしました: {output_path}")
        else:
            # 標準出力
            click.echo(data)
        
    except Exception as e:
        logger.error(f"エクスポートでエラーが発生しました: {e}")
        click.echo(f"❌ エラー: {e}")
        raise click.Abort()


@cli.command()
def test():
    """システムのテストを実行"""
    try:
        click.echo("🧪 システムテスト開始")
        
        # 設定の読み込みテスト
        click.echo("1. 設定ファイルの読み込み...")
        config = config_manager.config
        click.echo(f"   ✅ 設定読み込み完了: {len(config.dict())}項目")
        
        # カレンダークライアントのテスト
        click.echo("2. カレンダークライアントのテスト...")
        from app.adapters.calendar_client import GoogleCalendarClient
        calendar_client = GoogleCalendarClient()
        permissions = calendar_client.check_permissions()
        click.echo(f"   {'✅' if permissions else '❌'} カレンダーアクセス権限: {'あり' if permissions else 'なし'}")
        
        # スプレッドシートクライアントのテスト
        click.echo("3. スプレッドシートクライアントのテスト...")
        from app.adapters.sheets_client import GoogleSheetsClient
        sheets_client = GoogleSheetsClient()
        permissions = sheets_client.check_permissions()
        click.echo(f"   {'✅' if permissions else '❌'} スプレッドシートアクセス権限: {'あり' if permissions else 'なし'}")
        
        # 抽出器のテスト
        click.echo("4. 抽出器のテスト...")
        from app.core.rules import RuleBasedExtractor
        from app.core.extractor import AIExtractor
        
        rule_extractor = RuleBasedExtractor()
        test_event = {
            'title': '【B】株式会社サンプル / 田中様 / オンライン面談',
            'description': '営業アポの面談',
            'location': 'Zoom',
            'attendees': [{'displayName': '田中太郎', 'email': 'tanaka@sample.co.jp'}]
        }
        
        extracted = rule_extractor.extract_from_event(test_event)
        click.echo(f"   ✅ 規則ベース抽出テスト: 会社名={extracted.company_name}, 人名={extracted.person_names}")
        
        click.echo("\n🎉 システムテスト完了")
        
    except Exception as e:
        logger.error(f"テストでエラーが発生しました: {e}")
        click.echo(f"❌ エラー: {e}")
        raise click.Abort()


@cli.command()
@click.option('--interval', default=15, help='定期実行の間隔（分）')
def schedule(interval: int):
    """定期同期を実行"""
    import time
    from datetime import datetime
    
    click.echo(f"定期同期を開始します（間隔: {interval}分）")
    click.echo("Ctrl+Cで停止")
    
    try:
        while True:
            click.echo(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 同期実行")
            try:
                sync_service = CalendarSyncService()
                result = sync_service.sync_calendar_to_sheets()
                click.echo(f"同期完了: {result.upserted}件更新, {result.skipped}件スキップ")
            except Exception as e:
                click.echo(f"同期エラー: {e}")
            
            click.echo(f"次回実行まで{interval}分待機...")
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        click.echo("\n定期同期を停止しました")


@cli.command()
@click.option('--from-simple', is_flag=True, help='Bookings_SimpleからカレンダーへPushする')
def push(from_simple: bool):
    """スプレッドシートからカレンダーへ反映"""
    if from_simple:
        try:
            from app.adapters.sheets_client import GoogleSheetsClient
            from app.adapters.calendar_client import GoogleCalendarClient
            from dateutil import tz
            from datetime import datetime

            sheets = GoogleSheetsClient()
            cal = GoogleCalendarClient()

            records = sheets.read_simple_rows()
            if not records:
                click.echo('No rows in Bookings_Simple')
                return

            tokyo = tz.gettz('Asia/Tokyo')

            for idx, rec in enumerate(records, start=2):
                event_id = (rec.get('event_id') or '').strip()
                date_str = rec.get('date')
                company = (rec.get('company_name') or '').strip()
                persons = (rec.get('person_names') or '').strip()

                if not date_str:
                    continue

                # 13:00-14:00のデフォルト時間帯
                start_dt = datetime.fromisoformat(date_str).replace(hour=13, minute=0, second=0, tzinfo=tokyo)
                end_dt = start_dt.replace(hour=14)
                start_iso = start_dt.isoformat()
                end_iso = end_dt.isoformat()

                title = f"【B】{company}・{persons}" if company else f"【B】{persons}"

                if event_id:
                    cal.update_event(event_id=event_id, summary=title, start_iso=start_iso, end_iso=end_iso)
                else:
                    new_id = cal.create_event(summary=title, start_iso=start_iso, end_iso=end_iso)
                    if new_id:
                        sheets.write_simple_event_id(row_index=idx, event_id=new_id)

            click.echo('Push from Bookings_Simple completed')
        except Exception as e:
            click.echo(f'Error: {e}')
            raise click.Abort()
    else:
        click.echo('Specify --from-simple to push from Bookings_Simple')

@cli.command()
def config():
    """設定情報を表示"""
    try:
        config = config_manager.config
        
        click.echo("⚙️  設定情報")
        click.echo("=" * 50)
        
        # カレンダー設定
        click.echo("📅 カレンダー設定")
        click.echo(f"  カレンダーID: {config.calendar['calendar_id']}")
        click.echo(f"  過去同期日数: {config.calendar['sync_window_past_days']}日")
        click.echo(f"  未来同期日数: {config.calendar['sync_window_future_days']}日")
        click.echo(f"  最大取得件数: {config.calendar['max_results']}件")
        
        # イベントフィルタ設定
        click.echo("\n🔍 イベントフィルタ設定")
        click.echo(f"  【B】パターン: {config.event_filter['b_event_pattern']}")
        click.echo(f"  括弧バリエーション許可: {config.event_filter['allow_bracket_variations']}")
        
        # AI抽出設定
        click.echo("\n🤖 AI抽出設定")
        click.echo(f"  プロバイダー: {config.ai_extraction['provider']}")
        click.echo(f"  モデル: {config.ai_extraction['model']}")
        click.echo(f"  信頼度閾値: {config.ai_extraction['confidence_threshold']}")
        click.echo(f"  最大リトライ回数: {config.ai_extraction['max_retries']}")
        
        # スプレッドシート設定
        click.echo("\n📊 スプレッドシート設定")
        click.echo(f"  シート名: {config.spreadsheet['sheet_name']}")
        click.echo(f"  バッチサイズ: {config.spreadsheet['batch_size']}")
        
        # 同期設定
        click.echo("\n🔄 同期設定")
        click.echo(f"  実行間隔: {config.sync['interval_minutes']}分")
        click.echo(f"  リトライ間隔: {config.sync['retry_interval_seconds']}秒")
        click.echo(f"  最大リトライ回数: {config.sync['max_retries']}")
        
        # ログ設定
        click.echo("\n📝 ログ設定")
        click.echo(f"  レベル: {config.logging['level']}")
        click.echo(f"  ファイル出力: {config.logging['file_output']}")
        if config.logging['file_output']:
            click.echo(f"  ログファイル: {config.logging['log_file']}")
        
    except Exception as e:
        logger.error(f"設定表示でエラーが発生しました: {e}")
        click.echo(f"❌ エラー: {e}")
        raise click.Abort()


def _display_sync_result(result):
    """同期結果を表示"""
    click.echo(f"\n📊 同期結果")
    click.echo("=" * 30)
    click.echo(f"実行ID: {result.run_id}")
    click.echo(f"開始時刻: {result.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    click.echo(f"終了時刻: {result.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    click.echo(f"実行時間: {result.duration_seconds:.2f}秒")
    click.echo(f"総イベント数: {result.total_events}件")
    click.echo(f"【B】イベント数: {result.matched_b_events}件")
    click.echo(f"更新件数: {result.upserted}件")
    click.echo(f"スキップ件数: {result.skipped}件")
    click.echo(f"エラー件数: {result.errors}件")
    click.echo(f"成功率: {result.success_rate:.1%}")


if __name__ == '__main__':
    cli()
