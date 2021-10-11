<?php

namespace FOD\DBALClickHouse;

use Doctrine\ORM\Mapping\ClassMetadataFactory;

class ClickHouseMetadataFactory extends ClassMetadataFactory
{
	/**
	 * Forces the factory to load the metadata of all classes known to the underlying
	 * mapping driver.
	 *
	 * @return \Doctrine\Persistence\Mapping\ClassMetadata[] The ClassMetadata instances of all mapped classes.
	 */
	public function getAllMetadata()
	{
		if (! $this->initialized) {
			$this->initialize();
		}

		$driver   = $this->getDriver();
		$metadata = [];
		foreach ($driver->getAllClassNames() as $className)
		{
			$item = $this->getMetadataFor($className);

			$metadata[] = $item;
		}

		return $metadata;
	}

}
