/*
 * Copyright (c) 2017, Miguel Gamboa
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.github.jayield.rapper;

import com.github.jayield.rapper.utils.UnitOfWork;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author Miguel Gamboa
 *         created on 24-03-2017
 */
public class Countify {

    static class Counter<T, R> implements ICounter<T, R> {
        private int count=0;
        private final BiFunction<UnitOfWork, T, R> inner;

        public Counter(BiFunction<UnitOfWork, T, R> inner) {
            this.inner = inner;
        }

        @Override
        public int getCount() {
            return count;
        }

        @Override
        public R apply(UnitOfWork unit, T arg) {
            count++;
            return inner.apply(unit, arg);
        }
    }

    public static <T,R> ICounter of(BiFunction<UnitOfWork, T, R> inner) {
        return new Counter<>(inner);
    }
}
