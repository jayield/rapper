package rapper;

import org.github.isel.rapper.DomainObject;
import org.github.isel.rapper.EmbeddedId;

public class Car implements DomainObject<Car.PrimaryPk> {

    @EmbeddedId
    private final PrimaryPk pk;

    public Car(int owner, String plate, String brand, String model) {
        this.pk = new PrimaryPk(owner, plate);
        this.brand = brand;
        this.model = model;
    }

    private final String brand;
    private final String model;



    @Override
    public PrimaryPk getIdentityKey() {
        return null;
    }

    public static class PrimaryPk{
        private final int owner;

        public PrimaryPk(int owner, String plate) {
            this.owner = owner;
            this.plate = plate;
        }

        private final String plate;
    }
}
