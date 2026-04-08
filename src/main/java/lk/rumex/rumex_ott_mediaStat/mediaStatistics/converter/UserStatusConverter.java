package lk.rumex.rumex_ott_mediaStat.mediaStatistics.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import lk.rumex.rumex_ott_mediaStat.mediaStatistics.Enum.UserStatus;

@Converter(autoApply = false)
public class UserStatusConverter implements AttributeConverter<UserStatus, String> {

    @Override
    public String convertToDatabaseColumn(UserStatus attribute) {
        UserStatus safe = attribute == null ? UserStatus.R : attribute;
        return safe.name();
    }

    @Override
    public UserStatus convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.trim().isEmpty()) {
            return UserStatus.R;
        }

        String normalized = dbData.trim().toUpperCase();
        if ("PG-13".equals(normalized)) {
            normalized = "PG_13";
        }

        try {
            return UserStatus.valueOf(normalized);
        } catch (IllegalArgumentException ex) {
            return UserStatus.R;
        }
    }
}
