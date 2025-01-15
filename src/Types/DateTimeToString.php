<?php
/**
 * Created by PhpStorm.
 * User: ageneralov
 * Date: 2019-02-15
 * Time: 17:03
 */

namespace FOD\DBALClickHouse\Types;

use Doctrine\DBAL\Platforms\AbstractPlatform;
class DateTimeToString extends \DateTime
{
	public function __toString()
	{
		return $this->format("Y-m-d");
	}

	public function requiresSQLCommentHint(AbstractPlatform $platform)
	{
		return true;
	}
}