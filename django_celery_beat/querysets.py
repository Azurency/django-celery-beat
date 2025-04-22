"""Model querysets."""

try:
    from zoneinfo import ZoneInfo, available_timezones
except ImportError:
    from backports.zoneinfo import available_timezones, ZoneInfo

from datetime import datetime, timedelta

from django.db import models
from django.db.models import Case, When, Value, F, ExpressionWrapper, IntegerField, CharField
from django.db.models.functions import Cast, Floor, Mod


def get_timezone_offsets():
    timezone_offsets = {}
    for tz_name in available_timezones():
        try:
            tz = ZoneInfo(tz_name)
            # Use arbitrary date to get the offset
            now = datetime.now(tz)
            utc_offset = now.utcoffset()
            offset_sec = utc_offset.total_seconds()
            timezone_offsets[tz_name] = offset_sec
        except Exception as e:
            # In case of any error, continue to the next timezone
            continue
    return timezone_offsets


def annotate_with_utc_hour_minute(queryset, timezone_field, hour_field, minute_field, prefix=''):
    """
    Annotate queryset with UTC hour and minute, handling crontab expressions.
    :param queryset: The base queryset
    :param timezone_field: Field name for timezone (e.g., 'timezone' or 'crontab__timezone')
    :param hour_field: Field name for hour (e.g., 'hour' or 'crontab__hour')
    :param minute_field: Field name for minute (e.g., 'minute' or 'crontab__minute')
    :param prefix: Optional prefix for related fields (e.g., 'crontab__')
    :return: Annotated queryset
    """
    timezone_offsets = get_timezone_offsets()
    if not timezone_offsets:
        return queryset
    offset_annotation = {
        'offset': Case(
            *[
                When(**{timezone_field: tz}, then=Value(timezone_offsets.get(tz, 0)))
                for tz in timezone_offsets.keys()
            ],
            default=Value(0),
            output_field=IntegerField()
        )
    }
    utc_hour_annotation = {
        'utc_hour': Case(
            When(
                **{f"{hour_field}__regex": r"^\d+$"},
                then=Cast(
                    Mod(
                        ExpressionWrapper(
                            Cast(F(hour_field), output_field=IntegerField()) -
                            Floor(F("offset") / Value(3600)),
                            output_field=IntegerField(),
                        ),
                        Value(24),
                    ),
                    output_field=CharField(),
                ),
            ),
            default=F(hour_field),
            output_field=CharField(),
        )
    }
    utc_minute_annotation = {
        'utc_minute': Case(
            When(
                **{f"{minute_field}__regex": r"^\d+$"},
                then=Cast(
                    ExpressionWrapper(
                        Cast(F(minute_field), output_field=IntegerField()) -
                        Floor(Mod(F("offset"), Value(3600)) / Value(60)),
                        output_field=IntegerField(),
                    ),
                    output_field=CharField(),
                ),
            ),
            default=F(minute_field),
            output_field=CharField(),
        )
    }
    return queryset.annotate(**offset_annotation).annotate(**utc_hour_annotation, **utc_minute_annotation)


class PeriodicTaskQuerySet(models.QuerySet):
    """QuerySet for PeriodicTask."""

    def enabled(self):
        return self.filter(enabled=True).prefetch_related(
            "interval", "crontab", "solar", "clocked"
        )
    
    def with_crontab_utc_hour_minute(self):
        return annotate_with_utc_hour_minute(
            self,
            timezone_field='crontab__timezone',
            hour_field='crontab__hour',
            minute_field='crontab__minute'
        )


class CrontabScheduleQuerySet(models.QuerySet):
    """QuerySet for CrontabSchedule."""

    def with_utc_hour_minute(self):
        return annotate_with_utc_hour_minute(
            self,
            timezone_field='timezone',
            hour_field='hour',
            minute_field='minute'
        )
