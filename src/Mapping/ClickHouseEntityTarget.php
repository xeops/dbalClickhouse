<?php
/**
 * Created by PhpStorm.
 * User: ageneralov
 * Date: 05.06.2018
 * Time: 21:25
 */

namespace ClickHouseBundle\Mapping;

/**
 * @Annotation
 * @Target("CLASS")
 */
class ClickHouseEntityTarget
{
	public $entityClass;
}
