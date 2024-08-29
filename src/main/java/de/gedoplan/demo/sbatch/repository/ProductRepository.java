package de.gedoplan.demo.sbatch.repository;

import de.gedoplan.demo.sbatch.repository.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;

@Repository
public interface ProductRepository extends JpaRepository<Product, BigDecimal> {
}
